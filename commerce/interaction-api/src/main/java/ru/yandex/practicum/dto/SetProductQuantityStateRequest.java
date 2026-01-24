package ru.yandex.practicum.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.enums.QuantityState;

import java.util.UUID;

/**
 * DTO для запроса на изменение состояния количества товара.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SetProductQuantityStateRequest {
    /**
     * Уникальный идентификатор товара.
     */
    @NotNull(message = "Product ID cannot be null")
    private UUID productId;

    /**
     * Новое состояние количества товара.
     */
    @NotNull(message = "Quantity state cannot be null")
    private QuantityState quantityState;
}