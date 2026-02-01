package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.config.DeliveryProperties;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.enums.DeliveryState;
import ru.yandex.practicum.exception.delivery.DeliveryNotFoundException;
import ru.yandex.practicum.feign.OrderFeignClient;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.mapper.DeliveryMapper;
import ru.yandex.practicum.model.Delivery;
import ru.yandex.practicum.repository.DeliveryRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Сервис для управления бизнес-логикой доставок.
 * Обрабатывает создание, обновление статусов и расчет стоимости доставок.
 */
@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final WarehouseFeignClient warehouseClient;
    private final OrderFeignClient orderClient;
    private final DeliveryProperties deliveryProperties;

    /**
     * Создает новую доставку на основе предоставленных данных.
     *
     * @param deliveryDto данные для создания доставки
     * @return созданная доставка в формате DTO
     * @throws IllegalArgumentException если доставка для заказа уже существует
     */
    @Transactional
    public DeliveryDto createNewDelivery(DeliveryDto deliveryDto) {
        log.info("Creating new delivery for order: {}", deliveryDto.getOrderId());

        if (deliveryRepository.existsByOrderId(deliveryDto.getOrderId())) {
            throw new IllegalArgumentException("Delivery already exists for order: " + deliveryDto.getOrderId());
        }

        Delivery delivery = deliveryMapper.mapToDelivery(deliveryDto);
        Delivery savedDelivery = deliveryRepository.save(delivery);

        log.info("Delivery created successfully: {}", savedDelivery.getDeliveryId());
        return deliveryMapper.mapToDeliveryDto(savedDelivery);
    }

    /**
     * Изменяет статус доставки на "Доставлено".
     *
     * @param deliveryId идентификатор доставки
     * @throws DeliveryNotFoundException если доставка не найдена
     * @throws IllegalStateException     если текущий статус не позволяет завершить доставку
     */
    @Transactional
    public void changeStatusDeliveryOnDelivered(UUID deliveryId) {
        log.info("Changing delivery status to DELIVERED: {}", deliveryId);

        Delivery delivery = getDeliveryByIdEntity(deliveryId);

        if (!delivery.canCompleteDelivery()) {
            throw new IllegalStateException(
                    "Delivery cannot be marked as DELIVERED. Current state: " + delivery.getDeliveryState()
            );
        }

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        orderClient.deliveryOrder(delivery.getOrderId());
        log.info("Delivery status changed to DELIVERED: {}", deliveryId);
    }

    /**
     * Помечает товары как собранные и начинает доставку.
     *
     * @param deliveryId идентификатор доставки
     * @throws DeliveryNotFoundException если доставка не найдена
     * @throws IllegalStateException     если текущий статус не позволяет начать доставку
     */
    @Transactional
    public void pickedProductsOnDelivery(UUID deliveryId) {
        log.info("Picking products for delivery: {}", deliveryId);

        Delivery delivery = getDeliveryByIdEntity(deliveryId);

        if (!delivery.canStartDelivery()) {
            throw new IllegalStateException(
                    "Delivery cannot be started. Current state: " + delivery.getDeliveryState()
            );
        }

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        // Временная заглушка - пустой Map продуктов
        // В реальной реализации нужно получить список продуктов для заказа
        Map<UUID, Integer> products = new HashMap<>();

        // Используем существующий метод из WarehouseFeignClient
        warehouseClient.getProductOnOrderForDelivery(products, delivery.getOrderId());

        log.info("Products picked for delivery: {}", deliveryId);
    }

    /**
     * Изменяет статус доставки на "Не удалось".
     *
     * @param deliveryId идентификатор доставки
     * @throws DeliveryNotFoundException если доставка не найдена
     * @throws IllegalStateException     если текущий статус не позволяет отметить как неудачную
     */
    @Transactional
    public void changeStatusDeliveryOnFailed(UUID deliveryId) {
        log.info("Changing delivery status to FAILED: {}", deliveryId);

        Delivery delivery = getDeliveryByIdEntity(deliveryId);

        if (!delivery.canCancelDelivery()) {
            throw new IllegalStateException(
                    "Delivery cannot be marked as FAILED. Current state: " + delivery.getDeliveryState()
            );
        }

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        orderClient.deliveryOrderFailed(delivery.getOrderId());
        log.info("Delivery status changed to FAILED: {}", deliveryId);
    }

    /**
     * Рассчитывает стоимость доставки для указанного заказа.
     *
     * @param orderDto данные заказа для расчета
     * @return рассчитанная стоимость доставки
     * @throws DeliveryNotFoundException если доставка для заказа не найдена
     * @throws IllegalArgumentException  если данные адреса некорректны
     */
    public Double calculationCoastDelivery(OrderDto orderDto) {
        log.info("Calculating delivery cost for order: {}", orderDto.getOrderId());

        Delivery delivery = deliveryRepository.findByOrderId(orderDto.getOrderId())
                .orElseThrow(() -> new DeliveryNotFoundException(
                        "Delivery for order " + orderDto.getOrderId() + " not found"
                ));

        if (delivery.getFromAddress() == null || delivery.getToAddress() == null ||
                delivery.getFromAddress().getStreet() == null || delivery.getToAddress().getStreet() == null) {
            throw new IllegalArgumentException("The address cannot be null");
        }

        double deliveryCost = deliveryProperties.getCost().getBase();

        // Наценка в зависимости от склада
        double warehouseMarkup = fromAddressToString(delivery)
                .contains(deliveryProperties.getWarehouse().getAddressIndicator())
                ? deliveryProperties.getMarkup().getWarehouse().getHigh()
                : deliveryProperties.getMarkup().getWarehouse().getLow();
        deliveryCost += deliveryProperties.getCost().getBase() * warehouseMarkup;

        // Наценка за хрупкие товары
        if (Boolean.TRUE.equals(orderDto.getFragile())) {
            deliveryCost += deliveryCost * deliveryProperties.getMarkup().getFragilePercentage();
        }

        // Наценка за вес и объем
        if (orderDto.getDeliveryWeight() != null && orderDto.getDeliveryVolume() != null) {
            deliveryCost += orderDto.getDeliveryWeight() * deliveryProperties.getCost().getWeightPerUnit()
                    + orderDto.getDeliveryVolume() * deliveryProperties.getCost().getVolumePerUnit();
        }

        // Наценка за доставку на другую улицу
        if (!isSameStreet(delivery)) {
            deliveryCost += deliveryCost * deliveryProperties.getMarkup().getDifferentStreetPercentage();
        }

        log.info("Calculated delivery cost: {} for order: {}", deliveryCost, orderDto.getOrderId());
        return deliveryCost;
    }

    /**
     * Получает доставку по её идентификатору.
     *
     * @param deliveryId идентификатор доставки
     * @return доставка в формате DTO
     * @throws DeliveryNotFoundException если доставка не найдена
     */
    public DeliveryDto getDeliveryById(UUID deliveryId) {
        log.info("Getting delivery by ID: {}", deliveryId);
        Delivery delivery = getDeliveryByIdEntity(deliveryId);
        return deliveryMapper.mapToDeliveryDto(delivery);
    }

    /**
     * Получает доставку по идентификатору заказа.
     *
     * @param orderId идентификатор заказа
     * @return доставка в формате DTO
     * @throws DeliveryNotFoundException если доставка для заказа не найдена
     */
    public DeliveryDto getDeliveryByOrderId(UUID orderId) {
        log.info("Getting delivery by order ID: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryNotFoundException(
                        "Delivery for order " + orderId + " not found"
                ));

        return deliveryMapper.mapToDeliveryDto(delivery);
    }

    /**
     * Вспомогательный метод для получения сущности доставки по ID.
     *
     * @param deliveryId идентификатор доставки
     * @return сущность Delivery
     * @throws DeliveryNotFoundException если доставка не найдена
     */
    private Delivery getDeliveryByIdEntity(UUID deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new DeliveryNotFoundException(
                        "Delivery with ID = " + deliveryId + " not found"
                ));
    }

    /**
     * Вспомогательный метод для получения строкового представления адреса отправления.
     *
     * @param delivery сущность доставки
     * @return строковое представление адреса отправления
     */
    private String fromAddressToString(Delivery delivery) {
        return delivery.getFromAddress() != null ?
                delivery.getFromAddress().toString() : "";
    }

    /**
     * Вспомогательный метод для проверки, совпадают ли улицы отправления и доставки.
     *
     * @param delivery сущность доставки
     * @return true если улицы совпадают
     */
    private boolean isSameStreet(Delivery delivery) {
        if (delivery.getFromAddress() == null || delivery.getToAddress() == null) {
            return false;
        }
        String fromStreet = delivery.getFromAddress().getStreet().trim().toLowerCase();
        String toStreet = delivery.getToAddress().getStreet().trim().toLowerCase();
        return fromStreet.equals(toStreet);
    }
}