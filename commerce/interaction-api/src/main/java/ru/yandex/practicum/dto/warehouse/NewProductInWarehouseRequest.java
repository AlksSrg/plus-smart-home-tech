package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * DTO для запроса на добавление нового товара на склад.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NewProductInWarehouseRequest {

    /**
     * Уникальный идентификатор товара.
     */
    @NotNull(message = "ID товара не может быть null")
    private UUID productId;

    /**
     * Признак хрупкости товара.
     */
    private Boolean fragile;

    /**
     * Габаритные размеры товара.
     */
    @NotNull(message = "Размеры не могут быть null")
    private DimensionDto dimension;

    /**
     * Вес товара.
     * Должен быть не менее 1.
     */
    @NotNull(message = "Вес не может быть null")
    @Min(value = 1, message = "Вес должен быть не менее 1")
    private Double weight;
}