package ru.yandex.practicum.enums;

/**
 * Перечисление состояний количества товара.
 * Используется для индикации доступности товара на складе.
 */
public enum QuantityState {

    ENDED,
    FEW,
    ENOUGH,
    MANY
}