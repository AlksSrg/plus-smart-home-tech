package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для представления корзины покупок.
 * Содержит информацию о корзине и товарах в ней.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCartDto {
    /**
     * Уникальный идентификатор корзины.
     */
    @NotNull
    private UUID cartId;

    /**
     * Товары в корзине.
     * Ключ - ID товара, значение - количество.
     */
    @NotNull
    private Map<UUID, @NotNull @Positive Integer> products;
}