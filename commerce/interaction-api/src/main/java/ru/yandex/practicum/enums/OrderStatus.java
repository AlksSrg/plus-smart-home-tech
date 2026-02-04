package ru.yandex.practicum.enums;

/**
 * Перечисление статусов заказа.
 * Определяет все возможные состояния заказа в системе.
 */
public enum OrderStatus {
    NEW,
    ON_PAYMENT,
    ON_DELIVERY,
    DONE,
    DELIVERED,
    ASSEMBLED,
    PAID,
    COMPLETED,
    DELIVERY_FAILED,
    ASSEMBLY_FAILED,
    PAYMENT_FAILED,
    PRODUCT_RETURNED,
    CANCELED
}