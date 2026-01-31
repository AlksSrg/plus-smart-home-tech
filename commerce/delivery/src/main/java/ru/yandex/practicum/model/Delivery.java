package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.enums.DeliveryState;

import java.util.UUID;

/**
 * Сущность для представления доставки в базе данных.
 * Хранит информацию о доставке, включая адреса и статус.
 */
@Entity
@Table(name = "deliveries")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Delivery {

    /**
     * Уникальный идентификатор доставки.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "delivery_id")
    private UUID deliveryId;

    /**
     * Адрес отправления. Использует каскадное сохранение.
     */
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "from_address_id")
    private Address fromAddress;

    /**
     * Адрес доставки. Использует каскадное сохранение.
     */
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "to_address_id")
    private Address toAddress;

    /**
     * Идентификатор связанного заказа.
     */
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    /**
     * Текущий статус доставки.
     */
    @Column(name = "delivery_state", nullable = false)
    @Enumerated(value = EnumType.STRING)
    private DeliveryState deliveryState;

    /**
     * Проверяет, можно ли начать доставку.
     *
     * @return true если статус CREATED
     */
    public boolean canStartDelivery() {
        return deliveryState == DeliveryState.CREATED;
    }

    /**
     * Проверяет, можно ли завершить доставку.
     *
     * @return true если статус IN_PROGRESS
     */
    public boolean canCompleteDelivery() {
        return deliveryState == DeliveryState.IN_PROGRESS;
    }

    /**
     * Проверяет, можно ли отменить доставку.
     *
     * @return true если статус CREATED или IN_PROGRESS
     */
    public boolean canCancelDelivery() {
        return deliveryState == DeliveryState.CREATED ||
                deliveryState == DeliveryState.IN_PROGRESS;
    }
}