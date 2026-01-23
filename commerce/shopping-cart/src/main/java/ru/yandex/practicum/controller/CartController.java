package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.service.CartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Контроллер для управления корзиной покупок.
 * Предоставляет REST API для операций с корзиной пользователя.
 */
@Slf4j
@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class CartController {
    /**
     * Сервис для работы с корзиной покупок.
     */
    private final CartService cartService;

    /**
     * Получить актуальную корзину для авторизованного пользователя.
     *
     * @param username имя пользователя
     * @return корзина покупок в формате DTO
     */
    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam String username) {
        log.info("Method getShoppingCart: user name {}", username);
        return cartService.getShoppingCart(username);
    }

    /**
     * Добавить товар в корзину.
     *
     * @param username имя пользователя
     * @param products карта товаров для добавления (ID товара → количество)
     * @return обновленная корзина покупок
     */
    @PutMapping
    public ShoppingCartDto addProductInCart(
            @RequestParam String username,
            @RequestBody @NotEmpty Map<UUID, @NotNull @Positive Integer> products
    ) {
        log.info("Method addProductInCart: user name = {} and body product = {}", username, products);
        return cartService.addProductInCart(username, products);
    }

    /**
     * Деактивация корзины товаров для пользователя.
     *
     * @param username имя пользователя
     */
    @DeleteMapping
    public void deactivationCart(@RequestParam String username) {
        log.info("Method deactivationCart: user name {}", username);
        cartService.deactivationCart(username);
    }

    /**
     * Удалить указанные товары из корзины пользователя.
     *
     * @param username    имя пользователя
     * @param productsIds список ID товаров для удаления
     * @return обновленная корзина покупок
     */
    @PostMapping("/remove")
    public ShoppingCartDto removeProductFromCart(
            @RequestParam String username,
            @RequestBody @NotEmpty List<UUID> productsIds
    ) {
        log.info("Method removeProductFromCart: products Id {} and user name {}", productsIds, username);
        return cartService.removeProductFromCart(username, productsIds);
    }

    /**
     * Изменить количество товаров в корзине.
     *
     * @param username        имя пользователя
     * @param quantityRequest запрос на изменение количества товара
     * @return обновленная корзина покупок
     */
    @PostMapping("/change-quantity")
    public ShoppingCartDto changeQuantityInCart(
            @RequestParam String username,
            @Valid @RequestBody ChangeProductQuantityRequest quantityRequest
    ) {
        log.info("Method changeQuantityInCart: user name {}", username);
        return cartService.changeQuantityInCart(username, quantityRequest);
    }
}