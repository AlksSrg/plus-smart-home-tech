package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для запроса на возврат заказа.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductReturnRequest {

    /**
     * Идентификатор заказа для возврата.
     */
    private UUID orderId;

    /**
     * Товары для возврата (идентификатор товара -> количество).
     */
    @NotNull(message = "Список товаров не может быть null")
    @NotEmpty(message = "Список товаров не может быть пустым")
    private Map<UUID, Integer> products;
}