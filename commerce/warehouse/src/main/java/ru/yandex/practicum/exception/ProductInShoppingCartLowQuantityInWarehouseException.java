package ru.yandex.practicum.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.util.Map;
import java.util.UUID;

@Getter
public class ProductInShoppingCartLowQuantityInWarehouseException extends BaseWarehouseException {

    private final Map<UUID, Integer> unavailableProducts;

    public ProductInShoppingCartLowQuantityInWarehouseException(String message,
                                                                Map<UUID, Integer> unavailableProducts) {
        super(message, HttpStatus.BAD_REQUEST);
        this.unavailableProducts = unavailableProducts;
    }
}