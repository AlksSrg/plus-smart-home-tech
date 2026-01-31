package ru.yandex.practicum.dto.order;

import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.enums.OrderStatus;

/**
 * DTO для запроса на обновление статуса заказа.
 * Используется для изменения текущего статуса заказа с указанием причины.
 */
@Data
@Builder
public class OrderStatusUpdateRequest {

    /**
     * Новый статус заказа.
     * Определяет состояние, в которое должен перейти заказ.
     */
    private OrderStatus status;

    /**
     * Причина изменения статуса заказа.
     * Используется для логирования и информирования пользователя о причинах изменений.
     */
    private String reason;
}