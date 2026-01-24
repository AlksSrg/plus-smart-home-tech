package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.SetProductQuantityStateRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Feign-клиент для взаимодействия с сервисом корзины покупок.
 * Определяет REST-эндпоинты для операций с корзиной.
 */
@FeignClient(
        name = "shopping-cart",
        path = "/api/v1/shopping-cart",
        configuration = FeignClientConfig.class
)
public interface CartFeignClient {

    /**
     * Получить корзину покупок пользователя.
     *
     * @param userName имя пользователя
     * @return пагинированный список товаров в корзине
     * @throws FeignException при ошибке обращения к сервису
     */
    @GetMapping
    PageProductDTO getShoppingCart(@RequestParam String userName) throws FeignException;

    /**
     * Добавить товары в корзину.
     *
     * @param userName имя пользователя
     * @param products Map товаров для добавления (ID товара → количество)
     * @return обновленный пагинированный список товаров в корзине
     * @throws FeignException при ошибке обращения к сервису
     */
    @PutMapping
    PageProductDTO addProductInCart(
            @RequestParam String userName,
            @RequestBody @NotEmpty Map<UUID, @NotNull @Positive Integer> products
    ) throws FeignException;

    /**
     * Деактивировать корзину пользователя.
     *
     * @param userName имя пользователя
     * @throws FeignException при ошибке обращения к сервису
     */
    @DeleteMapping
    void deactivationCart(@RequestParam String userName) throws FeignException;

    /**
     * Удалить товары из корзины.
     *
     * @param userName    имя пользователя
     * @param productsIds список ID товаров для удаления
     * @return обновленный пагинированный список товаров в корзине
     * @throws FeignException при ошибке обращения к сервису
     */
    @PostMapping("/remove")
    PageProductDTO removeProductFromCart(
            @RequestParam String userName,
            @RequestBody @NotEmpty List<UUID> productsIds
    ) throws FeignException;

    /**
     * Изменить количество товара в корзине.
     *
     * @param userName        имя пользователя
     * @param quantityRequest запрос на изменение состояния количества товара
     * @return обновленный пагинированный список товаров в корзине
     * @throws FeignException при ошибке обращения к сервису
     */
    @PostMapping("/change-quantity")
    PageProductDTO changeQuantityInCart(
            @RequestParam String userName,
            @Valid @RequestBody SetProductQuantityStateRequest quantityRequest
    ) throws FeignException;
}