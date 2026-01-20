package ru.yandex.practicum.service;

import jakarta.ws.rs.BadRequestException;
import lombok.RequiredArgsConstructor;
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

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartService {
    private final CartRepository cartRepository;
    private final CartMapper mapper;
    private final WarehouseFeignClient warehouseFeignClient;

    @Transactional
    public ShoppingCartDto getShoppingCart(String username) {
        checkUsernameOnEmpty(username);
        ShoppingCart cart = getOrCreateCart(username);
        return mapper.mapToCartDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductInCart(String username, Map<UUID, Integer> products) {
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

    @Transactional
    public void deactivationCart(String username) {
        checkUsernameOnEmpty(username);

        ShoppingCart cart = getOrCreateCart(username);
        cart.setStatus(ShoppingCartStatus.DEACTIVATE);
    }

    @Transactional
    public ShoppingCartDto removeProductFromCart(String username, List<UUID> productsIds) {
        checkUsernameOnEmpty(username);

        ShoppingCart cart = getOrCreateCart(username);
        validateCartStatus(cart);

        validateCartHaveAllProduct(cart, productsIds);

        productsIds.forEach(id -> cart.getProducts().remove(id));

        return mapper.mapToCartDto(cart);
    }

    @Transactional
    public ShoppingCartDto changeQuantityInCart(String username, ChangeProductQuantityRequest quantityRequest) {
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

        cart.getProducts().forEach((id, count) -> cart.getProducts().put(id, count));

        return mapper.mapToCartDto(cart);
    }

    private void checkUsernameOnEmpty(String username) {
        if (username == null || username.isBlank()) throw new NotAuthorizedUserException("Username is empty");
    }

    private ShoppingCart getOrCreateCart(String username) {
        return cartRepository.findByUsername(username)
                .orElseGet(() -> {
                    ShoppingCart cart = ShoppingCart.builder()
                            .username(username)
                            .build();
                    cartRepository.save(cart);
                    return cart;
                });
    }

    private void validateCartStatus(ShoppingCart cart) {
        if (cart == null) throw new BadRequestException("The shopping cart was not found");

        if (cart.getStatus() == null) throw new IllegalStateException("The status of the bucket is not defined");

        if (cart.getStatus().equals(ShoppingCartStatus.DEACTIVATE))
            throw new CartDeactivateException("The user's shopping cart is deactivated");
    }

    private void checkAvailableProductsInWarehouse(UUID shoppingCartId, Map<UUID, Integer> products) {
        ShoppingCartDto shoppingCartDto = ShoppingCartDto.builder()
                .cartId(shoppingCartId)
                .products(products)
                .build();

        warehouseFeignClient.checkProductQuantity(shoppingCartDto);
    }

    private void validateCartHaveAllProduct(ShoppingCart shoppingCart, Collection<UUID> productsIds) {
        int quantityProductInCart = shoppingCart.getProducts().size();
        int quantityProductToCheck = productsIds.size();

        if (quantityProductToCheck > quantityProductInCart)
            throw new NoProductsInCartException("The number of items is more than in the basket");

        List<UUID> notFoundIds = new ArrayList<>();
        productsIds.forEach(id -> {
            if (!shoppingCart.getProducts().containsKey(id)) notFoundIds.add(id);
        });

        if (!notFoundIds.isEmpty())
            throw new NoProductsInCartException("Found items that are not in the shopping cart.");
    }
}