package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.model.Order;

/**
 * Маппер для преобразования между сущностью Order и DTO.
 * Использует MapStruct для автоматической генерации кода преобразования.
 */
@Mapper(componentModel = "spring")
public interface OrderMapper {

    /**
     * Преобразует сущность Order в DTO OrderDto.
     *
     * @param order сущность заказа
     * @return DTO с информацией о заказе
     */
    OrderDto toResponse(Order order);
}