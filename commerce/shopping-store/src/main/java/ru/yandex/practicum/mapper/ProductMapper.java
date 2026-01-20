package ru.yandex.practicum.mapper;

import org.mapstruct.*;
import org.springframework.data.domain.Page;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.ProductDTO;
import ru.yandex.practicum.entity.ProductEntity;

import java.util.List;
import java.util.stream.Collectors;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ProductMapper {

    ProductDTO toDTO(ProductEntity entity);

    ProductEntity toEntity(ProductDTO dto);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    void updateProductFromDto(@MappingTarget ProductEntity entity, ProductDTO dto);

    default PageProductDTO toPageDTO(Page<ProductEntity> page) {
        if (page == null) return null;

        List<ProductDTO> content = page.getContent().stream()
                .map(this::toDTO)
                .collect(Collectors.toList());

        return PageProductDTO.builder()
                .content(content)
                .totalElements(page.getTotalElements())
                .totalPages(page.getTotalPages())
                .first(page.isFirst())
                .last(page.isLast())
                .size(page.getSize())
                .number(page.getNumber())
                .numberOfElements(page.getNumberOfElements())
                .empty(page.isEmpty())
                .sort(page.getSort())
                .build();
    }
}