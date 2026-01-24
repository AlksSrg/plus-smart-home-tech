package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.model.ShoppingCart;

/**
 * Маппер для преобразования между сущностью ShoppingCart и DTO ShoppingCartDto.
 * Использует MapStruct для автоматической генерации кода.
 */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface CartMapper {

    /**
     * Преобразует сущность ShoppingCart в DTO ShoppingCartDto.
     *
     * @param shoppingCart сущность корзины покупок
     * @return DTO корзины покупок
     */
    @Mapping(source = "cartId", target = "cartId")
    @Mapping(source = "products", target = "products")
    ShoppingCartDto mapToCartDto(ShoppingCart shoppingCart);
}