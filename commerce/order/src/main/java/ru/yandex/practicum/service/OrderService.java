package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.enums.OrderStatus;
import ru.yandex.practicum.exception.order.NoOrderFoundException;
import ru.yandex.practicum.exception.order.ServiceUnavailableException;
import ru.yandex.practicum.exception.shoppingCart.NotAuthorizedUserException;
import ru.yandex.practicum.exception.warehouse.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.mapper.OrderMapper;
import ru.yandex.practicum.model.Order;
import ru.yandex.practicum.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Сервис для управления заказами.
 * Содержит бизнес-логику работы с заказами и интеграцию с другими сервисами.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final ShoppingCartService shoppingCartService;
    private final WarehouseFeignClient warehouseFeignClient;
    private final PriceCalculatorService priceCalculatorService;

    /**
     * Создает новый заказ из корзины пользователя.
     *
     * @param username    имя пользователя
     * @param createOrder DTO с данными для создания заказа
     * @return созданный заказ
     * @throws NotAuthorizedUserException             если имя пользователя пустое
     * @throws IllegalArgumentException               если корзина пуста
     * @throws NoSpecifiedProductInWarehouseException если товаров нет на складе
     */
    @Transactional
    public OrderDto createNewOrder(String username, CreateNewOrderRequest createOrder) {
        log.info("Создание заказа для пользователя: {}", username);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        ShoppingCartDto cart = createOrder.getShoppingCart();

        if (cart.getProducts() == null || cart.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Корзина пуста");
        }

        BookedProductsDto warehouseInfo;
        try {
            warehouseInfo = warehouseFeignClient.checkQuantityProducts(cart);
        } catch (Exception e) {
            throw new NoSpecifiedProductInWarehouseException("Нет заказываемого товара на складе", e);
        }

        Order order = Order.builder()
                .username(username)
                .cartId(cart.getShoppingCartId())
                .items(cart.getProducts())
                .status(OrderStatus.NEW)
                .volume(warehouseInfo.getDeliveryVolume())
                .weight(warehouseInfo.getDeliveryWeight())
                .isFragile(warehouseInfo.getFragile())
                .build();

        calculatePrices(order);

        Order savedOrder = orderRepository.save(order);

        try {
            shoppingCartService.deactivateCart(username);
        } catch (Exception e) {
            log.warn("Не удалось деактивировать корзину: {}", e.getMessage());
        }

        log.info("Заказ создан: {}", savedOrder.getOrderId());
        return orderMapper.toResponse(savedOrder);
    }

    /**
     * Получает список заказов пользователя с пагинацией.
     *
     * @param username имя пользователя
     * @param pageable параметры пагинации
     * @return страница с заказами пользователя
     * @throws NotAuthorizedUserException если имя пользователя пустое
     */
    public Page<OrderDto> getOrderByUsername(String username, Pageable pageable) {
        log.debug("Получение заказов пользователя: {}", username);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }

        return orderRepository.findByUsername(username, pageable)
                .map(orderMapper::toResponse);
    }

    /**
     * Получает заказ по идентификатору.
     *
     * @param orderId идентификатор заказа
     * @return информация о заказе
     * @throws NoOrderFoundException если заказ не найден
     */
    public OrderDto getOrderById(UUID orderId) {
        log.debug("Получение заказа по ID: {}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));
        return orderMapper.toResponse(order);
    }

    /**
     * Инициирует возврат товаров заказа.
     *
     * @param productReturn запрос на возврат товаров
     * @return обновленный заказ
     * @throws NoOrderFoundException    если заказ не найден
     * @throws IllegalStateException    если заказ уже возвращен или отменен
     * @throws IllegalArgumentException если список товаров пуст
     */
    @Transactional
    public OrderDto returnOrderProducts(ProductReturnRequest productReturn) {
        log.info("Возврат товаров заказа: {}", productReturn.getOrderId());

        Order order = orderRepository.findById(productReturn.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + productReturn.getOrderId()));

        if (order.getStatus() == OrderStatus.PRODUCT_RETURNED || order.getStatus() == OrderStatus.CANCELED) {
            throw new IllegalStateException("Заказ уже возвращен или отменен");
        }

        if (productReturn.getProducts() == null || productReturn.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Список товаров для возврата пуст");
        }

        try {
            warehouseFeignClient.returnProductToWarehouse(productReturn.getProducts());
        } catch (Exception e) {
            throw new ServiceUnavailableException("Сервис склада недоступен", e);
        }

        order.setStatus(OrderStatus.PRODUCT_RETURNED);
        Order updated = orderRepository.save(order);

        return orderMapper.toResponse(updated);
    }

    /**
     * Инициирует сборку заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     * @throws NoOrderFoundException       если заказ не найден
     * @throws IllegalStateException       если заказ не может быть отправлен на сборку
     * @throws ServiceUnavailableException если сервис склада недоступен
     */
    @Transactional
    public OrderDto assemblyOrder(UUID orderId) {
        log.info("Сборка заказа: {}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getStatus() == OrderStatus.NEW) {
            try {
                warehouseFeignClient.getProductOnOrderForDelivery(order.getItems(), orderId);
            } catch (Exception e) {
                throw new ServiceUnavailableException("Сервис склада недоступен", e);
            }
            order.setStatus(OrderStatus.ASSEMBLED);
        } else {
            throw new IllegalStateException("Заказ не может быть отправлен на сборку");
        }

        return orderMapper.toResponse(orderRepository.save(order));
    }

    /**
     * Обработка неудачной сборки заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto assemblyOrderFailed(UUID orderId) {
        log.info("Ошибка сборки заказа: {}", orderId);
        return updateOrderStatus(orderId, OrderStatus.ASSEMBLY_FAILED);
    }

    /**
     * Инициирует оплату заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto paymentOrder(UUID orderId) {
        log.info("Оплата заказа: {}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getStatus() == OrderStatus.ON_PAYMENT) {
            order.setStatus(OrderStatus.PAID);
            return orderMapper.toResponse(order);
        }

        if (order.getStatus() == OrderStatus.PAID) {
            return orderMapper.toResponse(order);
        }

        if (order.getStatus() != OrderStatus.ASSEMBLED) {
            throw new IllegalStateException("Заказ не собран");
        }

        order.setStatus(OrderStatus.ON_PAYMENT);
        return orderMapper.toResponse(orderRepository.save(order));
    }

    /**
     * Обработка неудачной оплаты заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto paymentOrderFailed(UUID orderId) {
        log.info("Ошибка оплаты заказа: {}", orderId);
        return updateOrderStatus(orderId, OrderStatus.PAYMENT_FAILED);
    }

    /**
     * Инициирует доставку заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto deliveryOrder(UUID orderId) {
        log.info("Доставка заказа: {}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        if (order.getStatus() == OrderStatus.ON_DELIVERY) {
            order.setStatus(OrderStatus.DELIVERED);
            return orderMapper.toResponse(order);
        }

        if (order.getStatus() == OrderStatus.DELIVERED) {
            return orderMapper.toResponse(order);
        }

        if (order.getStatus() != OrderStatus.PAID) {
            throw new IllegalStateException("Доставка осуществляется только по предоплате");
        }

        order.setStatus(OrderStatus.ON_DELIVERY);
        return orderMapper.toResponse(orderRepository.save(order));
    }

    /**
     * Обработка неудачной доставки заказа.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto deliveryOrderFailed(UUID orderId) {
        log.info("Ошибка доставки заказа: {}", orderId);
        return updateOrderStatus(orderId, OrderStatus.DELIVERY_FAILED);
    }

    /**
     * Завершает заказ.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ
     */
    @Transactional
    public OrderDto completedOrder(UUID orderId) {
        log.info("Завершение заказа: {}", orderId);
        return updateOrderStatus(orderId, OrderStatus.COMPLETED);
    }

    /**
     * Рассчитывает итоговую стоимость заказа и возвращает обновленный заказ.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ с рассчитанной итоговой стоимостью
     * @throws NoOrderFoundException если заказ не найден
     */
    @Transactional
    public OrderDto calculateOrderTotalPrice(UUID orderId) {
        log.debug("Расчет итоговой стоимости для заказа: {}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        calculatePrices(order);
        Order updated = orderRepository.save(order);

        return orderMapper.toResponse(updated);
    }

    /**
     * Рассчитывает стоимость доставки для заказа и возвращает обновленный заказ.
     *
     * @param orderId идентификатор заказа
     * @return обновленный заказ с рассчитанной стоимостью доставки
     * @throws NoOrderFoundException если заказ не найден
     */
    @Transactional
    public OrderDto calculateOrderDeliveryPrice(UUID orderId) {
        log.debug("Расчет стоимости доставки для заказа: {}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        BigDecimal deliveryPrice = priceCalculatorService.calculateDeliveryPrice(
                order.getWeight(),
                order.getVolume(),
                order.getIsFragile()
        );

        order.setDeliveryPrice(deliveryPrice);
        Order updated = orderRepository.save(order);

        return orderMapper.toResponse(updated);
    }

    /**
     * Обновляет статус заказа.
     *
     * @param orderId   идентификатор заказа
     * @param newStatus новый статус заказа
     * @return обновленный заказ
     * @throws NoOrderFoundException если заказ не найден
     * @throws IllegalStateException если переход статуса недопустим
     */
    @Transactional
    private OrderDto updateOrderStatus(UUID orderId, OrderStatus newStatus) {
        log.info("Обновление статуса заказа {} на {}", orderId, newStatus);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));

        validateStatusTransition(order.getStatus(), newStatus);

        order.setStatus(newStatus);
        Order updated = orderRepository.save(order);

        return orderMapper.toResponse(updated);
    }

    /**
     * Рассчитывает цены для заказа.
     *
     * @param order заказ для расчета цен
     */
    private void calculatePrices(Order order) {
        BigDecimal itemsPrice = priceCalculatorService.calculateItemsPrice(order.getItems());
        BigDecimal deliveryPrice = priceCalculatorService.calculateDeliveryPrice(
                order.getWeight(),
                order.getVolume(),
                order.getIsFragile()
        );

        order.setItemsPrice(itemsPrice);
        order.setDeliveryPrice(deliveryPrice);
        order.setTotalPrice(itemsPrice.add(deliveryPrice));
    }

    /**
     * Проверяет допустимость перехода между статусами заказа.
     *
     * @param current текущий статус
     * @param next    следующий статус
     * @throws IllegalStateException если переход недопустим
     */
    private void validateStatusTransition(OrderStatus current, OrderStatus next) {
        if (current == OrderStatus.CANCELED || current == OrderStatus.COMPLETED) {
            throw new IllegalStateException(
                    String.format("Невозможно изменить статус заказа из %s в %s", current, next)
            );
        }

        if (current == OrderStatus.DELIVERY_FAILED &&
                (next != OrderStatus.CANCELED && next != OrderStatus.PRODUCT_RETURNED)) {
            throw new IllegalStateException(
                    "После неудачной доставки заказ можно только отменить или вернуть"
            );
        }
    }
}