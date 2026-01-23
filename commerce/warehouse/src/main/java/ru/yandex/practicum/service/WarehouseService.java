package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouseException;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.model.WarehouseItem;
import ru.yandex.practicum.repository.WarehouseItemRepository;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Сервис для управления операциями на складе.
 * Обеспечивает бизнес-логику работы со складскими товарами.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseService {

    private final WarehouseItemRepository repository;
    private final String warehouseAddress;

    /**
     * Добавляет новый товар на склад.
     *
     * @param request запрос с данными нового товара
     * @throws SpecifiedProductAlreadyInWarehouseException если товар уже существует на складе
     */
    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        log.info("Добавление нового товара на склад: {}", request.getProductId());

        if (repository.existsByProductId(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Товар с ID " + request.getProductId() + " уже зарегистрирован на складе"
            );
        }

        WarehouseItem item = WarehouseItem.builder()
                .productId(request.getProductId())
                .quantity(0)
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .weight(request.getWeight())
                .isFragile(request.getFragile() != null ? request.getFragile() : false)
                .createdAt(LocalDateTime.now())
                .build();

        repository.save(item);
        log.info("Товар {} добавлен на склад", request.getProductId());
    }

    /**
     * Проверяет доступность товаров на складе для корзины покупок.
     *
     * @param shoppingCartDto данные корзины покупок
     * @return DTO с информацией о доступных товарах для доставки
     * @throws ProductInShoppingCartLowQuantityInWarehouseException если товаров недостаточно на складе
     */
    @Transactional
    public BookedProductsDto checkProductQuantity(ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
        log.info("Проверка доступности товаров для корзины: {}", shoppingCartDto.getCartId());

        Map<UUID, Integer> unavailableProducts = new HashMap<>();
        Double totalWeight = 0.0;
        Double totalVolume = 0.0;
        Boolean hasFragileItems = false;

        for (Map.Entry<UUID, Integer> entry : shoppingCartDto.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Integer requestedQuantity = entry.getValue();

            Optional<WarehouseItem> itemOpt = repository.findByProductId(productId);

            if (itemOpt.isEmpty()) {
                unavailableProducts.put(productId, 0);
                continue;
            }

            WarehouseItem item = itemOpt.get();
            Integer availableQuantity = item.getQuantity();

            if (availableQuantity < requestedQuantity) {
                unavailableProducts.put(productId, requestedQuantity - availableQuantity);
                continue;
            }

            // Рассчитываем общие параметры доставки
            totalWeight += item.getWeight() * requestedQuantity;
            totalVolume += item.getVolume() * requestedQuantity;

            if (Boolean.TRUE.equals(item.getIsFragile())) {
                hasFragileItems = true;
            }
        }

        if (!unavailableProducts.isEmpty()) {
            throw new ProductInShoppingCartLowQuantityInWarehouseException(
                    "Недостаточно товаров на складе",
                    unavailableProducts
            );
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragileItems)
                .build();
    }

    /**
     * Добавляет дополнительное количество существующего товара на склад.
     *
     * @param request запрос с данными о добавляемом количестве товара
     * @throws NoSpecifiedProductInWarehouseException если товар не найден на складе
     */
    @Transactional
    public void addProductQuantity(AddProductToWarehouseRequest request) {
        log.info("Добавление количества товара: {}, количество: {}",
                request.getProductId(), request.getQuantity());

        WarehouseItem item = repository.findByProductId(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар с ID " + request.getProductId() + " не найден на складе"
                ));

        item.setQuantity(item.getQuantity() + request.getQuantity());
        item.setUpdatedAt(LocalDateTime.now());

        repository.save(item);
        log.info("Количество обновлено для товара: {}, новое количество: {}",
                request.getProductId(), item.getQuantity());
    }

    /**
     * Возвращает адрес склада.
     *
     * @return DTO с адресом склада
     */
    public AddressDto getWarehouseAddress() {
        log.info("Получение адреса склада: {}", warehouseAddress);


        return AddressDto.builder()
                .country(warehouseAddress)
                .city(warehouseAddress)
                .street(warehouseAddress)
                .house(warehouseAddress)
                .flat(warehouseAddress)
                .build();
    }

    /**
     * Получает информацию о товаре на складе.
     *
     * @param productId ID товара
     * @return сущность товара на складе
     * @throws NoSpecifiedProductInWarehouseException если товар не найден на складе
     */
    @Transactional(readOnly = true)
    public WarehouseItem getProductInfo(UUID productId) {
        return repository.findByProductId(productId)
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар с ID " + productId + " не найден на складе"
                ));
    }
}