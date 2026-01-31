package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.PageProductDto;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.QuantityState;

import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом магазина товаров (Shopping Store).
 * Предоставляет полный набор методов для работы с товарами, включая создание,
 * обновление, удаление, получение и управление состоянием товаров.
 */
@FeignClient(
        name = "shopping-store",
        path = "/api/v1/shopping-store",
        configuration = FeignClientConfig.class
)
public interface ShoppingStoreClient {

    /**
     * Получает пагинированный список товаров по указанной категории.
     * Использует GET запрос для получения товаров с фильтрацией по категории
     * и поддержкой пагинации.
     *
     * @param category категория товаров для фильтрации, не может быть null
     * @param pageable параметры пагинации (страница, размер, сортировка)
     * @return пагинированный список товаров указанной категории
     * @throws FeignException в случае ошибки HTTP-запроса или недоступности сервиса
     */
    @GetMapping
    PageProductDto getAllProducts(
            @RequestParam ProductCategory category,
            Pageable pageable
    ) throws FeignException;

    /**
     * Получает информацию о конкретном товаре по его идентификатору.
     * Этот метод предоставляет альтернативный endpoint для получения товара
     * с использованием другого URL пути.
     *
     * @param productId уникальный идентификатор товара, не может быть null
     * @return DTO объект с полной информацией о товаре
     * @throws FeignException в случае если товар не найден или произошла ошибка сервиса
     */
    @GetMapping("/{productId}")
    ProductDto getProductById(@PathVariable UUID productId) throws FeignException;

    /**
     * Получает информацию о товаре по идентификатору через альтернативный endpoint.
     * Этот метод предоставляет дополнительный endpoint для совместимости
     * с существующими вызовами.
     *
     * @param productId уникальный идентификатор товара, не может быть null
     * @return DTO объект с полной информацией о товаре
     * @throws FeignException в случае если товар не найден или произошла ошибка сервиса
     */
    @GetMapping("/products/{productId}")
    ProductDto getProductByIdAlternative(@PathVariable UUID productId) throws FeignException;

    /**
     * Создает новый товар в магазине.
     * Использует PUT запрос для создания нового товара с валидацией входных данных.
     *
     * @param productDTO DTO объект с данными для создания нового товара, должен быть валидным
     * @return DTO объект созданного товара с присвоенным идентификатором
     * @throws FeignException в случае некорректных данных или ошибки сервиса
     */
    @PutMapping
    ProductDto createProduct(@Valid @RequestBody ProductDto productDTO) throws FeignException;

    /**
     * Обновляет существующий товар в магазине.
     * Использует POST запрос для обновления данных товара. Все поля товара
     * должны быть указаны в запросе.
     *
     * @param productDto DTO объект с обновленными данными товара, должен быть валидным
     * @return DTO объект обновленного товара
     * @throws FeignException в случае если товар не найден или данные некорректны
     */
    @PostMapping
    ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) throws FeignException;

    /**
     * Удаляет товар из магазина по его идентификатору.
     * Использует POST запрос с телом запроса для удаления товара.
     *
     * @param productId уникальный идентификатор товара для удаления, не может быть null
     * @return {@code true} если товар успешно удален, {@code false} в противном случае
     * @throws FeignException в случае ошибки сервиса или проблем с валидацией
     */
    @PostMapping("/removeProductFromStore")
    Boolean removeProductById(@Valid @RequestBody UUID productId) throws FeignException;

    /**
     * Устанавливает состояние количества для указанного товара.
     * Используется для управления наличием товара (например, IN_STOCK, OUT_OF_STOCK).
     *
     * @param productId     уникальный идентификатор товара, не может быть null
     * @param quantityState новое состояние количества товара, не может быть null
     * @return {@code true} если состояние успешно изменено, {@code false} в противном случае
     * @throws FeignException в случае если товар не найден или произошла ошибка сервиса
     */
    @PostMapping("/quantityState")
    Boolean setProductQuantityState(
            @RequestParam UUID productId,
            @RequestParam QuantityState quantityState
    ) throws FeignException;
}