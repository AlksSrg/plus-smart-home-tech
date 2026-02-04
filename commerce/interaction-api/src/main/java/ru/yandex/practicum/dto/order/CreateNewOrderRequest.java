package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;

/**
 * DTO для запроса на создание нового заказа.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateNewOrderRequest {

    /**
     * Корзина товаров в онлайн магазине.
     */
    @NotNull(message = "Корзина не может быть null")
    private ShoppingCartDto shoppingCart;

    /**
     * Представление адреса в системе.
     */
    @NotNull(message = "Адрес доставки не может быть null")
    private AddressDto deliveryAddress;
}