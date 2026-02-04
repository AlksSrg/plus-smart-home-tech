package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.enums.PaymentState;

import java.util.UUID;

/**
 * Сущность платежа в системе интернет-магазина.
 * Хранит информацию о платежах, их статусе, стоимости и связи с заказами.
 * Соответствует таблице "payments" в базе данных.
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    /**
     * Уникальный идентификатор платежа.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id")
    private UUID paymentId;

    /**
     * Идентификатор связанного заказа.
     */
    @Column(name = "order_id")
    private UUID orderId;

    /**
     * Общая сумма платежа.
     */
    @Column(name = "total_payment")
    private Double totalPayment;

    /**
     * Стоимость доставки.
     */
    @Column(name = "delivery_total")
    private Double deliveryTotal;

    /**
     * Сумма налога (НДС).
     */
    @Column(name = "fee_total")
    private Double feeTotal;

    /**
     * Статус платежа.
     */
    @Column(name = "state")
    @Enumerated(value = EnumType.STRING)
    private PaymentState state;
}