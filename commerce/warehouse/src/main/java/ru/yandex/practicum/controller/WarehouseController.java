package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.service.WarehouseService;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController {


    private final WarehouseService warehouseService;

    @PutMapping
    public ResponseEntity<Void> addNewProduct(@Valid @RequestBody NewProductInWarehouseRequest request) {
        log.info("PUT /api/v1/warehouse - Add new product: {}", request.getProductId());
        warehouseService.addNewProduct(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/check")
    public ResponseEntity<BookedProductsDto> checkProductQuantity(
            @Valid @RequestBody ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
        log.info("POST /api/v1/warehouse/check - Check availability for cart: {}",
                shoppingCartDto.getCartId());
        BookedProductsDto result = warehouseService.checkProductQuantity(shoppingCartDto);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/add")
    public ResponseEntity<Void> addProductQuantity(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("POST /api/v1/warehouse/add - Add quantity for product: {}",
                request.getProductId());
        warehouseService.addProductQuantity(request);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/address")
    public ResponseEntity<AddressDto> getWarehouseAddress() {
        log.info("GET /api/v1/warehouse/address - Get warehouse address");
        AddressDto address = warehouseService.getWarehouseAddress();
        return ResponseEntity.ok(address);
    }
}