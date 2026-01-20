package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.service.CartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class CartController {
    private final CartService cartService;

    @GetMapping
    public ResponseEntity<ShoppingCartDto> getShoppingCart(
            @RequestParam String username) {
        log.info("Получение корзины для пользователя: {}", username);
        ShoppingCartDto cart = cartService.getShoppingCart(username);
        return ResponseEntity.ok(cart);
    }

    @PutMapping
    public ResponseEntity<ShoppingCartDto> addProductInCart(
            @RequestParam String username,
            @RequestBody @NotEmpty Map<UUID, @NotNull @Positive Integer> products) {
        log.info("Добавление товаров в корзину пользователя: {}, товары: {}", username, products);
        ShoppingCartDto cart = cartService.addProductInCart(username, products);
        return ResponseEntity.ok(cart);
    }

    @DeleteMapping
    public ResponseEntity<Void> deactivationCart(
            @RequestParam String username) {
        log.info("Деактивация корзины пользователя: {}", username);
        cartService.deactivationCart(username);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/remove")
    public ResponseEntity<ShoppingCartDto> removeProductFromCart(
            @RequestParam String username,
            @RequestBody @NotEmpty List<UUID> productsIds) {
        log.info("Удаление товаров из корзины пользователя: {}, ID товаров: {}", username, productsIds);
        ShoppingCartDto cart = cartService.removeProductFromCart(username, productsIds);
        return ResponseEntity.ok(cart);
    }

    @PostMapping("/change-quantity")
    public ResponseEntity<ShoppingCartDto> changeQuantityInCart(
            @RequestParam String username,
            @Valid @RequestBody ChangeProductQuantityRequest quantityRequest) {
        log.info("Изменение количества товара для пользователя: {}, запрос: {}", username, quantityRequest);
        ShoppingCartDto cart = cartService.changeQuantityInCart(username, quantityRequest);
        return ResponseEntity.ok(cart);
    }
}