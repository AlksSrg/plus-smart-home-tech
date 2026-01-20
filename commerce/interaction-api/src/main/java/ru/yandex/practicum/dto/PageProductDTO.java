package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.domain.Sort;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class PageProductDTO {
    private long totalElements;
    private int totalPages;
    private boolean first;
    private boolean last;
    private int size;
    private List<ProductDTO> content;
    private int number;
    private Sort sort;
    private int numberOfElements;
    private PageableObject pageable;
    private boolean empty;
}