package ru.yandex.practicum.dto;

import lombok.Data;
import org.springframework.data.domain.Sort;

import java.util.List;

/**
 * DTO для представления информации о пагинации.
 * Используется для передачи параметров пагинации между слоями приложения.
 */
@Data
public class PageableObject {
    /**
     * Смещение (offset) для пагинации.
     */
    private long offset;

    /**
     * Список правил сортировки.
     */
    private List<Sort.Order> sort;

    /**
     * Флаг, указывающий что пагинация не используется.
     */
    private boolean unpaged;

    /**
     * Флаг, указывающий что пагинация используется.
     */
    private boolean paged;

    /**
     * Номер текущей страницы (начиная с 0).
     */
    private int pageNumber;

    /**
     * Размер страницы (количество элементов на странице).
     */
    private int pageSize;
}