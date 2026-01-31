package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для запроса на резервирование товаров для доставки заказа.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductsForOrderRequest {

    /**
     * Идентификатор заказа.
     */
    private UUID orderId;

    /**
     * Товары для резервирования (идентификатор товара -> количество).
     */
    private Map<UUID, Integer> products;
}