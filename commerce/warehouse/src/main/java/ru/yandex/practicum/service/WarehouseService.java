package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouseException;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.model.WarehouseItem;
import ru.yandex.practicum.repository.WarehouseItemRepository;

import java.time.LocalDateTime;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseService {

    private final WarehouseItemRepository repository;
    private final String warehouseAddress;

    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        log.info("Adding new product to warehouse: {}", request.getProductId());

        if (repository.existsByProductId(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Товар с ID " + request.getProductId() + " уже зарегистрирован на складе"
            );
        }

        WarehouseItem item = WarehouseItem.builder()
                .productId(request.getProductId())
                .quantity(0) // Изначально товара нет на складе
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .weight(request.getWeight())
                .isFragile(request.getFragile() != null ? request.getFragile() : false)
                .createdAt(LocalDateTime.now())
                .build();

        repository.save(item);
        log.info("Product {} added to warehouse", request.getProductId());
    }

    @Transactional
    public BookedProductsDto checkProductQuantity(ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
        log.info("Checking product availability for cart: {}", shoppingCartDto.getCartId());

        Map<UUID, Integer> unavailableProducts = new HashMap<>();
        Double totalWeight = 0.0;
        Double totalVolume = 0.0;
        Boolean hasFragileItems = false;

        for (Map.Entry<UUID, Integer> entry : shoppingCartDto.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Integer requestedQuantity = entry.getValue();

            Optional<WarehouseItem> itemOpt = repository.findByProductId(productId);

            if (itemOpt.isEmpty()) {
                unavailableProducts.put(productId, 0); // Товар вообще не найден на складе
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

    @Transactional
    public void addProductQuantity(AddProductToWarehouseRequest request) {
        log.info("Adding quantity for product: {}, quantity: {}",
                request.getProductId(), request.getQuantity());

        WarehouseItem item = repository.findByProductId(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар с ID " + request.getProductId() + " не найден на складе"
                ));

        item.setQuantity(item.getQuantity() + request.getQuantity());
        item.setUpdatedAt(LocalDateTime.now());

        repository.save(item);
        log.info("Quantity updated for product: {}, new quantity: {}",
                request.getProductId(), item.getQuantity());
    }

    public AddressDto getWarehouseAddress() {
        log.info("Getting warehouse address: {}", warehouseAddress);

        // Дублируем адрес во все поля согласно ТЗ
        return AddressDto.builder()
                .country(warehouseAddress)
                .city(warehouseAddress)
                .street(warehouseAddress)
                .house(warehouseAddress)
                .flat(warehouseAddress)
                .build();
    }

    @Transactional(readOnly = true)
    public WarehouseItem getProductInfo(UUID productId) {
        return repository.findByProductId(productId)
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар с ID " + productId + " не найден на складе"
                ));
    }
}