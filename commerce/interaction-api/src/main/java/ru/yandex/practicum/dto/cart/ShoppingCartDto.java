package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * Корзина товаров в онлайн магазине.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCartDto {

    /**
     * Идентификатор корзины в БД.
     */
    @NotNull(message = "Идентификатор корзины не может быть null")
    private UUID shoppingCartId;

    /**
     * Отображение идентификатора товара на отобранное количество.
     */
    @NotNull(message = "Товары не могут быть null")
    private Map<UUID, Integer> products;
}