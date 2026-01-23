package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.ProductDTO;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.QuantityState;

import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом товаров.
 * Используется для межсервисного взаимодействия.
 */
@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface ShoppingStoreClient {

    /**
     * Получает список товаров по категории.
     *
     * @param category Категория товаров
     * @param pageable Параметры пагинации
     * @return Список товаров
     * @throws FeignException в случае ошибки
     */
    @GetMapping
    PageProductDTO getAllProducts(
            @RequestParam ProductCategory category,
            Pageable pageable
    ) throws FeignException;

    /**
     * Создает новый товар.
     *
     * @param productDTO DTO товара
     * @return Созданный товар
     * @throws FeignException в случае ошибки
     */
    @PutMapping
    ProductDTO createProduct(@Valid @RequestBody ProductDTO productDTO) throws FeignException;

    /**
     * Обновляет существующий товар.
     *
     * @param productDTO DTO с обновленными данными
     * @return Обновленный товар
     * @throws FeignException в случае ошибки
     */
    @PostMapping
    ProductDTO updateProduct(@Valid @RequestBody ProductDTO productDTO) throws FeignException;

    /**
     * Удаляет товар по идентификатору.
     *
     * @param productId UUID идентификатор
     * @return true если успешно
     * @throws FeignException в случае ошибки
     */
    @PostMapping("/removeProductFromStore")
    Boolean removeProductById(@Valid @RequestBody UUID productId) throws FeignException;

    /**
     * Устанавливает состояние количества товара.
     *
     * @param productId     UUID идентификатор
     * @param quantityState Состояние количества
     * @return true если успешно
     * @throws FeignException в случае ошибки
     */
    @PostMapping("/quantityState")
    Boolean setProductQuantityState(
            @RequestParam UUID productId,
            @RequestParam QuantityState quantityState
    ) throws FeignException;

    /**
     * Получает товар по идентификатору.
     *
     * @param productId UUID идентификатор
     * @return Товар
     * @throws FeignException в случае ошибки
     */
    @GetMapping("/{productId}")
    ProductDTO getProductById(@PathVariable UUID productId) throws FeignException;
}