package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * DTO для запроса на изменение количества товара в корзине.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeProductQuantityRequest {
    /**
     * Уникальный идентификатор товара.
     */
    @NotNull
    private UUID productId;

    /**
     * Новое количество товара.
     */
    @NotNull
    private Integer newQuantity;
}