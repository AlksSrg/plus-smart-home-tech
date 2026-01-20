package ru.yandex.practicum.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductState;
import ru.yandex.practicum.enums.QuantityState;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
public class ProductDTO {
    private UUID productId;

    @NotBlank(message = "Название продукта не может быть пустым")
    private String productName;

    @NotBlank(message = "Описание не может быть пустым")
    private String description;

    private String imageSrc;

    @NotNull(message = "Состояние количества обязательно")
    private QuantityState quantityState;

    @NotNull(message = "Состояние продукта обязательно")
    private ProductState productState;

    private ProductCategory productCategory;

    @NotNull(message = "Цена обязательна")
    @Min(value = 1, message = "Цена должна быть не менее 1")
    private BigDecimal price;
}