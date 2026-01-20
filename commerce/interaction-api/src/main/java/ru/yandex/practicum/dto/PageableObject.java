package ru.yandex.practicum.dto;

import lombok.Data;
import org.springframework.data.domain.Sort;
import java.util.List;

@Data
public class PageableObject {
    private long offset;
    private List<Sort.Order> sort;
    private boolean unpaged;
    private boolean paged;
    private int pageNumber;
    private int pageSize;
}