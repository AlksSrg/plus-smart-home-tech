package ru.yandex.practicum.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.enums.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTO для представления информации о заказе.
 * Используется для ответов API с детальной информацией о заказе.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    /**
     * Уникальный идентификатор заказа.
     */
    private UUID orderId;

    /**
     * Текущий статус заказа.
     */
    private OrderStatus status;

    /**
     * Имя пользователя, создавшего заказ.
     */
    private String username;

    /**
     * Карта товаров в заказе (ID товара -> количество).
     */
    private Map<UUID, Integer> items;

    /**
     * Идентификатор корзины, из которой создан заказ.
     */
    private UUID cartId;

    /**
     * Идентификатор доставки.
     */
    private UUID deliveryId;

    /**
     * Идентификатор платежа.
     */
    private UUID paymentId;

    /**
     * Общий объем товаров в заказе (в кубических метрах).
     */
    private Double volume;

    /**
     * Общий вес товаров в заказе (в килограммах).
     */
    private Double weight;

    /**
     * Признак наличия хрупких товаров в заказе.
     */
    private Boolean isFragile;

    /**
     * Итоговая стоимость заказа.
     */
    private BigDecimal totalPrice;

    /**
     * Стоимость товаров в заказе.
     */
    private BigDecimal itemsPrice;

    /**
     * Стоимость доставки заказа.
     */
    private BigDecimal deliveryPrice;

    /**
     * Дата и время создания заказа.
     */
    private LocalDateTime createdAt;

    /**
     * Дата и время последнего обновления заказа.
     */
    private LocalDateTime updatedAt;
}