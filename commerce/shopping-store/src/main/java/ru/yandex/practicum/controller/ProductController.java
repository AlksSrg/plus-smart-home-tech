package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.PageProductDto;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.QuantityState;
import ru.yandex.practicum.service.ProductService;

import java.util.UUID;

/**
 * Контроллер для управления товарами в магазине.
 * Предоставляет REST API для операций с товарами.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class ProductController {

    private final ProductService productService;

    /**
     * Получает список товаров по категории.
     *
     * @param category Категория товаров
     * @param pageable Параметры пагинации и сортировки
     * @return Список товаров с информацией о пагинации
     */
    @GetMapping
    public PageProductDto getAllProducts(
            @RequestParam ProductCategory category,
            Pageable pageable) {

        log.info("Запрос списка товаров по категории: {}", category);
        return productService.getProductsByCategory(category, pageable);
    }

    /**
     * Создает новый товар.
     *
     * @param productDto DTO с данными товара
     * @return Созданный товар
     */
    @PutMapping
    public ProductDto createProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("Создание товара: {}", productDto.getProductName());
        return productService.createProduct(productDto);
    }

    /**
     * Обновляет существующий товар.
     *
     * @param productDto DTO с обновленными данными
     * @return Обновленный товар
     */
    @PostMapping
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("Обновление товара: {}", productDto.getProductName());
        return productService.updateProduct(productDto);
    }

    /**
     * Удаляет товар по идентификатору.
     *
     * @param productId UUID идентификатор товара
     * @return true если удаление успешно
     */
    @PostMapping("/removeProductFromStore")
    public Boolean removeProductById(@RequestBody UUID productId) {
        log.info("Удаление товара: ID={}", productId);
        return productService.removeProductById(productId);
    }

    /**
     * Устанавливает состояние количества товара.
     * Использует параметры запроса как в авторском решении.
     *
     * @param productId     UUID идентификатор товара
     * @param quantityState Новое состояние количества
     * @return true если операция успешна
     */
    @PostMapping("/quantityState")
    public Boolean setProductQuantityState(
            @RequestParam UUID productId,
            @RequestParam QuantityState quantityState) {

        log.info("Установка состояния количества: ID={}, состояние={}", productId, quantityState);

        SetProductQuantityStateRequest request = SetProductQuantityStateRequest.builder()
                .productId(productId)
                .quantityState(quantityState)
                .build();

        return productService.setProductQuantityState(request);
    }

    /**
     * Получает товар по идентификатору.
     *
     * @param productId UUID идентификатор товара
     * @return Товар
     */
    @GetMapping("/{productId}")
    public ProductDto getProductById(@PathVariable UUID productId) {
        log.info("Запрос товара: ID={}", productId);
        return productService.getProductById(productId);
    }
}