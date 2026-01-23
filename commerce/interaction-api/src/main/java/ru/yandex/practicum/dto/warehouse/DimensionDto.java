package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO для представления габаритных размеров товара.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimensionDto {

    /**
     * Ширина товара.
     * Должна быть не менее 1.
     */
    @NotNull(message = "Ширина не может быть null")
    @Min(value = 1, message = "Ширина должна быть не менее 1")
    private Double width;

    /**
     * Высота товара.
     * Должна быть не менее 1.
     */
    @NotNull(message = "Высота не может быть null")
    @Min(value = 1, message = "Высота должна быть не менее 1")
    private Double height;

    /**
     * Глубина товара.
     * Должна быть не менее 1.
     */
    @NotNull(message = "Глубина не может быть null")
    @Min(value = 1, message = "Глубина должна быть не менее 1")
    private Double depth;
}