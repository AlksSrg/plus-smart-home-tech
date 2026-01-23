package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для запроса проверки доступности товаров.
 * Не используется в текущем контроллере - сохранён для возможного расширения функционала.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseCheckRequestDto {

    /**
     * Идентификатор корзины покупок.
     */
    private UUID shoppingCartId;

    /**
     * Карта товаров для проверки.
     * Ключ - ID товара, значение - запрашиваемое количество.
     */
    private Map<UUID, Integer> products;
}