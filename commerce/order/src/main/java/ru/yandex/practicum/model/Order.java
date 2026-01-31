package ru.yandex.practicum.model;

import jakarta.persistence.*;
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
 * Сущность заказа в системе.
 * Хранит информацию о заказе пользователя, включая товары, стоимость и статус.
 */
@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    /**
     * Уникальный идентификатор заказа.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "order_id")
    private UUID orderId;

    /**
     * Текущий статус заказа.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    /**
     * Имя пользователя, создавшего заказ.
     */
    @Column(name = "username", nullable = false, length = 255)
    private String username;

    /**
     * Товары в заказе (идентификатор товара -> количество).
     */
    @ElementCollection
    @CollectionTable(
            name = "order_items",
            joinColumns = @JoinColumn(name = "order_id")
    )
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Integer> items;

    /**
     * Идентификатор корзины, из которой создан заказ.
     */
    @Column(name = "cart_id")
    private UUID cartId;

    /**
     * Идентификатор доставки.
     */
    @Column(name = "delivery_id")
    private UUID deliveryId;

    /**
     * Идентификатор оплаты.
     */
    @Column(name = "payment_id")
    private UUID paymentId;

    /**
     * Объем заказа в кубических метрах.
     */
    @Column(name = "volume", precision = 10)
    private Double volume;

    /**
     * Вес заказа в килограммах.
     */
    @Column(name = "weight", precision = 10)
    private Double weight;

    /**
     * Признак наличия хрупких товаров в заказе.
     */
    @Column(name = "is_fragile")
    private Boolean isFragile;

    /**
     * Итоговая стоимость заказа.
     */
    @Column(name = "total_price", precision = 10, scale = 2)
    private BigDecimal totalPrice;

    /**
     * Стоимость товаров в заказе.
     */
    @Column(name = "items_price", precision = 10, scale = 2)
    private BigDecimal itemsPrice;

    /**
     * Стоимость доставки заказа.
     */
    @Column(name = "delivery_price", precision = 10, scale = 2)
    private BigDecimal deliveryPrice;

    /**
     * Дата и время создания заказа.
     */
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    /**
     * Дата и время последнего обновления заказа.
     */
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    /**
     * Устанавливает дату создания и статус по умолчанию перед сохранением.
     */
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (status == null) {
            status = OrderStatus.NEW;
        }
    }

    /**
     * Обновляет дату изменения перед обновлением.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}