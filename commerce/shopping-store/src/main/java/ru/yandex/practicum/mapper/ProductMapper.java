package ru.yandex.practicum.mapper;

import org.mapstruct.*;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.entity.ProductEntity;

/**
 * Маппер для преобразования между сущностью и DTO товара.
 * Использует MapStruct для автоматического маппинга.
 */
@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface ProductMapper {

    /**
     * Преобразует сущность в DTO.
     *
     * @param entity Сущность товара
     * @return DTO товара
     */
    ProductDto toDTO(ProductEntity entity);

    /**
     * Преобразует DTO в сущность.
     *
     * @param dto DTO товара
     * @return Сущность товара
     */
    ProductEntity toEntity(ProductDto dto);

    /**
     * Обновляет сущность из DTO.
     *
     * @param entity Сущность для обновления
     * @param dto    DTO с новыми данными
     */
    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    void updateProductFromDto(@MappingTarget ProductEntity entity, ProductDto dto);
}