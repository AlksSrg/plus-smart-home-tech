package ru.yandex.practicum.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

import java.util.Map;
import java.util.UUID;

/**
 * Исключение, выбрасываемое при недостаточном количестве товара на складе
 * для оформления заказа из корзины.
 * Содержит информацию о товарах, которых не хватает.
 */
@Getter
public class ProductInShoppingCartLowQuantityInWarehouseException extends BaseWarehouseException {

    private final Map<UUID, Integer> unavailableProducts;

    /**
     * Конструктор исключения с сообщением и информацией о недостающих товарах.
     *
     * @param message             сообщение об ошибке
     * @param unavailableProducts карта с ID товаров и количеством, которого не хватает
     */
    public ProductInShoppingCartLowQuantityInWarehouseException(String message,
                                                                Map<UUID, Integer> unavailableProducts) {
        super(message, HttpStatus.BAD_REQUEST);
        this.unavailableProducts = unavailableProducts;
    }
}