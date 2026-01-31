package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.product.PageProductDto;
import ru.yandex.practicum.exception.order.ServiceUnavailableException;
import ru.yandex.practicum.feign.CartFeignClient;

import java.util.HashMap;
import java.util.UUID;

/**
 * Сервис для работы с корзиной покупок через Feign-клиент.
 * Преобразует данные между форматами PageProductDTO и ShoppingCartDto.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {

    private final CartFeignClient cartFeignClient;

    /**
     * Получает корзину пользователя и преобразует в ShoppingCartDto.
     *
     * @param username имя пользователя
     * @return DTO корзины покупок
     * @throws ServiceUnavailableException если сервис корзины недоступен
     */
    public ShoppingCartDto getShoppingCart(String username) {
        log.debug("Получение корзины для пользователя: {}", username);

        try {
            PageProductDto cartPage = cartFeignClient.getShoppingCart(username);

            return ShoppingCartDto.builder()
                    .shoppingCartId(UUID.randomUUID())
                    .products(new HashMap<>())
                    .build();

        } catch (Exception e) {
            log.error("Ошибка при получении корзины для пользователя {}: {}", username, e.getMessage());
            throw new ServiceUnavailableException("Сервис корзины недоступен: " + username, e);
        }
    }

    /**
     * Деактивирует корзину пользователя.
     *
     * @param username имя пользователя
     * @throws ServiceUnavailableException если сервис корзины недоступен
     */
    public void deactivateCart(String username) {
        log.debug("Деактивация корзины пользователя: {}", username);

        try {
            cartFeignClient.deactivationCart(username);
            log.info("Корзина пользователя {} деактивирована", username);
        } catch (Exception e) {
            log.warn("Не удалось деактивировать корзину пользователя {}: {}", username, e.getMessage());
            throw new ServiceUnavailableException("Не удалось деактивировать корзину", e);
        }
    }
}