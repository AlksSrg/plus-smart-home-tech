package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.domain.Sort;

import java.util.List;

/**
 * DTO для пагинированного списка товаров.
 * Содержит информацию о текущей странице и сортировке.
 */
@Data
@Builder
@AllArgsConstructor
public class PageProductDTO {
    /**
     * Список товаров на текущей странице.
     */
    private List<ProductDTO> content;

    /**
     * Информация о сортировке результатов.
     */
    private Sort sort;
}