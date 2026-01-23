package ru.yandex.practicum.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductState;
import ru.yandex.practicum.enums.QuantityState;

import java.util.UUID;

/**
 * DTO для представления товара.
 * Используется для передачи данных о товаре между клиентом и сервером.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDTO {
    /**
     * Уникальный идентификатор товара.
     */
    private UUID productId;

    /**
     * Название товара.
     */
    @NotBlank(message = "Название продукта не может быть пустым")
    private String productName;

    /**
     * Ссылка на изображение товара.
     */
    private String imageSrc;

    /**
     * Состояние количества товара (например, "много", "мало").
     */
    @NotNull(message = "Состояние количества обязательно")
    private QuantityState quantityState;

    /**
     * Состояние товара (активный/деактивированный).
     */
    @NotNull(message = "Состояние продукта обязательно")
    private ProductState productState;

    /**
     * Категория товара.
     */
    private ProductCategory productCategory;

    /**
     * Цена товара.
     */
    @Min(value = 1, message = "Цена должна быть не менее 1")
    @NotNull(message = "Цена обязательна")
    private Float price;

    /**
     * Описание товара.
     */
    @NotBlank(message = "Описание не может быть пустым")
    private String description;
}