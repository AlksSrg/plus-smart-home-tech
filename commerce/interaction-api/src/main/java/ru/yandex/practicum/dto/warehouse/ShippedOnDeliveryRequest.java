package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * DTO для запроса на передачу товаров в доставку
 * Используется при взаимодействии сервиса доставки со складом
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippedOnDeliveryRequest {

    /**
     * Идентификатор заказа, товары которого передаются в доставку
     * Не может быть null
     */
    @NotNull(message = "Идентификатор заказа обязателен")
    private UUID orderId;

    /**
     * Идентификатор доставки, в которую передаются товары
     * Не может быть null
     */
    @NotNull(message = "Идентификатор доставки обязателен")
    private UUID deliveryId;

    /**
     * Формирует строковое представление для логов
     *
     * @return Отформатированная строка с информацией о запросе
     */
    @Override
    public String toString() {
        return String.format("Запрос передачи в доставку [Заказ=%s, Доставка=%s]",
                orderId, deliveryId);
    }
}