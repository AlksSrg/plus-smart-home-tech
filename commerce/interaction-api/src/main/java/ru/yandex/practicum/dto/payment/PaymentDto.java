package ru.yandex.practicum.dto.payment;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.enums.PaymentState;

import java.util.UUID;

/**
 * DTO для представления информации о платеже.
 * Используется для передачи данных о платеже между сервисами.
 */
@Data
@Builder
public class PaymentDto {

    /**
     * Уникальный идентификатор платежа.
     */
    @NotNull
    private UUID paymentId;

    /**
     * Идентификатор связанного заказа.
     */
    private UUID orderId;

    /**
     * Статус платежа.
     */
    private PaymentState state;

    /**
     * Общая сумма платежа.
     */
    private Double totalPayment;

    /**
     * Стоимость доставки.
     */
    private Double deliveryTotal;

    /**
     * Сумма налога (НДС).
     */
    private Double feeTotal;
}