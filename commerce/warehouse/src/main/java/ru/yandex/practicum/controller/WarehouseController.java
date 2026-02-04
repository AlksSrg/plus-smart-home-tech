package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.service.WarehouseService;

/**
 * Контроллер для управления складскими операциями.
 * Предоставляет API для работы со складом товаров.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController {

    private final WarehouseService warehouseService;

    /**
     * Добавляет новый товар на склад.
     *
     * @param request запрос с данными нового товара
     * @return ResponseEntity с кодом 200 (OK) при успешном добавлении
     */
    @PutMapping
    public ResponseEntity<Void> addNewProduct(@Valid @RequestBody NewProductInWarehouseRequest request) {
        log.info("PUT /api/v1/warehouse - Добавление нового товара: {}", request.getProductId());
        warehouseService.addNewProduct(request);
        return ResponseEntity.ok().build();
    }

    /**
     * Проверяет, достаточно ли товаров на складе для оформления заказа из корзины.
     *
     * @param shoppingCartDto данные корзины товаров
     * @return ResponseEntity с данными о доступных для бронирования товарах
     */
    @PostMapping("/check")
    public ResponseEntity<BookedProductsDto> checkProductQuantity(
            @Valid @RequestBody ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
        log.info("POST /api/v1/warehouse/check - Проверка доступности товаров для корзины: {}",
                shoppingCartDto.getShoppingCartId());
        BookedProductsDto result = warehouseService.checkProductQuantity(shoppingCartDto);
        return ResponseEntity.ok(result);
    }

    /**
     * Принимает дополнительное количество товара на склад.
     *
     * @param request запрос с данными о добавляемом количестве товара
     * @return ResponseEntity с кодом 200 (OK) при успешном добавлении
     */
    @PostMapping("/add")
    public ResponseEntity<Void> addProductQuantity(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("POST /api/v1/warehouse/add - Добавление количества товара: {}",
                request.getProductId());
        warehouseService.addProductQuantity(request);
        return ResponseEntity.ok().build();
    }

    /**
     * Возвращает адрес склада для расчёта доставки.
     *
     * @return ResponseEntity с адресом склада
     */
    @GetMapping("/address")
    public ResponseEntity<AddressDto> getWarehouseAddress() {
        log.info("GET /api/v1/warehouse/address - Получение адреса склада");
        AddressDto address = warehouseService.getWarehouseAddress();
        return ResponseEntity.ok(address);
    }
}