package ru.yandex.practicum.dto;

import lombok.Data;

/**
 * DTO для представления информации о сортировке.
 * Используется для передачи параметров сортировки.
 */
@Data
public class SortObject {
    /**
     * Направление сортировки (ASC/DESC).
     */
    private String direction;

    /**
     * Обработка null значений.
     */
    private String nullHandling;

    /**
     * Флаг восходящей сортировки.
     */
    private boolean ascending;

    /**
     * Свойство для сортировки.
     */
    private String property;

    /**
     * Флаг игнорирования регистра при сортировке.
     */
    private boolean ignoreCase;
}