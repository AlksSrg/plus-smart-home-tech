package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Сущность корзины покупок.
 * Представляет корзину пользователя с товарами и их количеством.
 */
@Entity
@Table(name = "shopping_carts")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCart {
    /**
     * Уникальный идентификатор корзины.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "cart_id")
    private UUID cartId;

    /**
     * Имя пользователя, владельца корзины.
     */
    @Column(name = "username", nullable = false, unique = true)
    private String username;

    /**
     * Статус корзины (активна/деактивирована).
     */
    @Column(name = "status")
    @Enumerated(value = EnumType.STRING)
    @Builder.Default
    private ShoppingCartStatus status = ShoppingCartStatus.ACTIVE;

    /**
     * Товары в корзине: Map<ID товара, количество>.
     */
    @ElementCollection
    @CollectionTable(name = "cart_products", joinColumns = @JoinColumn(name = "cart_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    @Builder.Default
    private Map<UUID, Integer> products = new HashMap<>();

    /**
     * Дата и время создания корзины.
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();

    /**
     * Дата и время последнего обновления корзины.
     */
    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private LocalDateTime updatedAt = LocalDateTime.now();

    /**
     * Обновляет поле updatedAt при изменении сущности.
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}