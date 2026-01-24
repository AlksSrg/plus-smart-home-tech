package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * DTO для запроса на добавление количества существующего товара на склад.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddProductToWarehouseRequest {

    /**
     * Уникальный идентификатор товара.
     */
    @NotNull(message = "ID товара не может быть null")
    private UUID productId;

    /**
     * Количество товара для добавления.
     * Должно быть не менее 1.
     */
    @NotNull(message = "Количество не может быть null")
    @Min(value = 1, message = "Количество должно быть не менее 1")
    private Integer quantity;
}