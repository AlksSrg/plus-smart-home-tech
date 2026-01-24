package ru.yandex.practicum.service;

import jakarta.ws.rs.BadRequestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.CartDeactivateException;
import ru.yandex.practicum.exception.NoProductsInCartException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.mapper.CartMapper;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.model.ShoppingCartStatus;
import ru.yandex.practicum.repository.CartRepository;

import java.util.*;

/**
 * Сервис для управления корзиной покупок.
 * Содержит бизнес-логику работы с корзиной пользователя.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartService {
    private final CartRepository cartRepository;
    private final CartMapper mapper;
    private final WarehouseFeignClient warehouseFeignClient;

    /**
     * Получает корзину пользователя.
     * Если корзина не существует, создает новую.
     *
     * @param username имя пользователя
     * @return DTO корзины покупок
     */
    @Transactional
    public ShoppingCartDto getShoppingCart(String username) {
        log.debug("Getting shopping cart for user: {}", username);
        checkUsernameOnEmpty(username);
        ShoppingCart cart = getOrCreateCart(username);
        return mapper.mapToCartDto(cart);
    }

    /**
     * Добавляет товары в корзину пользователя.
     *
     * @param username имя пользователя
     * @param products Map товаров для добавления (ID товара → количество)
     * @return обновленная DTO корзины покупок
     * @throws BadRequestException если список товаров пуст
     */
    @Transactional
    public ShoppingCartDto addProductInCart(String username, Map<UUID, Integer> products) {
        log.debug("Adding products to cart for user: {}, products: {}", username, products);
        checkUsernameOnEmpty(username);

        if (products == null || products.isEmpty())
            throw new BadRequestException("The list of products cannot be empty");

        ShoppingCart cart = getOrCreateCart(username);
        validateCartStatus(cart);

        // Проверка доступности на складе через Feign-клиент
        checkAvailableProductsInWarehouse(cart.getCartId(), products);

        products.forEach(
                (productId, quantity) -> cart.getProducts().merge(productId, quantity, Integer::sum)
        );

        return mapper.mapToCartDto(cart);
    }

    /**
     * Деактивирует корзину пользователя.
     *
     * @param username имя пользователя
     */
    @Transactional
    public void deactivationCart(String username) {
        log.debug("Deactivating cart for user: {}", username);
        checkUsernameOnEmpty(username);

        ShoppingCart cart = getOrCreateCart(username);
        cart.setStatus(ShoppingCartStatus.DEACTIVATE);
    }

    /**
     * Удаляет товары из корзины пользователя.
     *
     * @param username    имя пользователя
     * @param productsIds список ID товаров для удаления
     * @return обновленная DTO корзины покупок
     */
    @Transactional
    public ShoppingCartDto removeProductFromCart(String username, List<UUID> productsIds) {
        log.debug("Removing products from cart for user: {}, product IDs: {}", username, productsIds);
        checkUsernameOnEmpty(username);

        ShoppingCart cart = getOrCreateCart(username);
        validateCartStatus(cart);

        validateCartHaveAllProduct(cart, productsIds);

        productsIds.forEach(id -> cart.getProducts().remove(id));

        return mapper.mapToCartDto(cart);
    }

    /**
     * Изменяет количество товара в корзине.
     *
     * @param username        имя пользователя
     * @param quantityRequest запрос на изменение количества
     * @return обновленная DTO корзины покупок
     * @throws BadRequestException если запрос или его поля пусты
     */
    @Transactional
    public ShoppingCartDto changeQuantityInCart(String username, ChangeProductQuantityRequest quantityRequest) {
        log.debug("Changing product quantity in cart for user: {}, request: {}", username, quantityRequest);
        checkUsernameOnEmpty(username);

        if (quantityRequest == null)
            throw new BadRequestException("The request to change the quantity cannot be empty");

        if (quantityRequest.getProductId() == null || quantityRequest.getNewQuantity() == null)
            throw new BadRequestException("ProductID and newQuantity must be filled in");

        ShoppingCart cart = getOrCreateCart(username);
        validateCartStatus(cart);

        validateCartHaveAllProduct(cart, List.of(quantityRequest.getProductId()));

        // Проверка доступности на складе через Feign-клиент
        checkAvailableProductsInWarehouse(
                cart.getCartId(),
                Map.of(quantityRequest.getProductId(), quantityRequest.getNewQuantity())
        );

        cart.getProducts().put(quantityRequest.getProductId(), quantityRequest.getNewQuantity());

        return mapper.mapToCartDto(cart);
    }

    /**
     * Проверяет, что имя пользователя не пустое.
     *
     * @param username имя пользователя
     * @throws NotAuthorizedUserException если имя пользователя пустое
     */
    private void checkUsernameOnEmpty(String username) {
        if (username == null || username.isBlank())
            throw new NotAuthorizedUserException("Username is empty");
    }

    /**
     * Получает или создает корзину для пользователя.
     *
     * @param username имя пользователя
     * @return существующая или новая корзина
     */
    private ShoppingCart getOrCreateCart(String username) {
        return cartRepository.findByUsername(username)
                .orElseGet(() -> {
                    ShoppingCart cart = ShoppingCart.builder()
                            .username(username)
                            .build();
                    cartRepository.save(cart);
                    log.debug("Created new cart for user: {}", username);
                    return cart;
                });
    }

    /**
     * Проверяет статус корзины.
     *
     * @param cart корзина для проверки
     * @throws BadRequestException     если корзина не найдена
     * @throws IllegalStateException   если статус корзины не определен
     * @throws CartDeactivateException если корзина деактивирована
     */
    private void validateCartStatus(ShoppingCart cart) {
        if (cart == null)
            throw new BadRequestException("The shopping cart was not found");

        if (cart.getStatus() == null)
            throw new IllegalStateException("The status of the bucket is not defined");

        if (cart.getStatus().equals(ShoppingCartStatus.DEACTIVATE))
            throw new CartDeactivateException("The user's shopping cart is deactivated");
    }

    /**
     * Проверяет доступность товаров на складе.
     *
     * @param shoppingCartId ID корзины
     * @param products       товары для проверки
     */
    private void checkAvailableProductsInWarehouse(UUID shoppingCartId, Map<UUID, Integer> products) {
        ShoppingCartDto shoppingCartDto = ShoppingCartDto.builder()
                .cartId(shoppingCartId)
                .products(products)
                .build();

        warehouseFeignClient.checkProductQuantity(shoppingCartDto);
        log.debug("Checked product availability in warehouse for cart: {}", shoppingCartId);
    }

    /**
     * Проверяет, что все указанные товары присутствуют в корзине.
     *
     * @param shoppingCart корзина для проверки
     * @param productsIds  ID товаров для проверки
     * @throws NoProductsInCartException если товары не найдены в корзине
     */
    private void validateCartHaveAllProduct(ShoppingCart shoppingCart, Collection<UUID> productsIds) {
        int quantityProductInCart = shoppingCart.getProducts().size();
        int quantityProductToCheck = productsIds.size();

        if (quantityProductToCheck > quantityProductInCart)
            throw new NoProductsInCartException("The number of items is more than in the basket");

        List<UUID> notFoundIds = new ArrayList<>();
        productsIds.forEach(id -> {
            if (!shoppingCart.getProducts().containsKey(id))
                notFoundIds.add(id);
        });

        if (!notFoundIds.isEmpty())
            throw new NoProductsInCartException("Found items that are not in the shopping cart.");
    }
}