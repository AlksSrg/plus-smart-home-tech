package ru.yandex.practicum.feign;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

/**
 * Feign клиент для взаимодействия с сервисом склада.
 * Определяет REST-эндпоинты для вызова операций склада.
 */
@FeignClient(
        name = "warehouse",
        path = "/api/v1/warehouse",
        configuration = FeignClientConfig.class,
        fallbackFactory = WarehouseFeignClientWithCircuitBreaker.class
)
public interface WarehouseFeignClient {

    /**
     * Добавляет новый товар на склад.
     *
     * @param request запрос с данными нового товара
     */
    @PutMapping
    @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackAddNewProduct")
    @Retry(name = "warehouse")
    void addNewProduct(@RequestBody NewProductInWarehouseRequest request);

    /**
     * Проверяет доступность товаров на складе для корзины покупок.
     *
     * @param shoppingCartDto данные корзины покупок
     * @return DTO с информацией о доступных товарах
     */
    @PostMapping("/check")
    @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackCheckProductQuantity")
    @Retry(name = "warehouse")
    BookedProductsDto checkProductQuantity(@RequestBody ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto);

    /**
     * Добавляет количество существующего товара на склад.
     *
     * @param request запрос с данными о добавляемом количестве
     */
    @PostMapping("/add")
    @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackAddProductQuantity")
    @Retry(name = "warehouse")
    void addProductQuantity(@RequestBody AddProductToWarehouseRequest request);

    /**
     * Получает адрес склада.
     *
     * @return DTO с адресом склада
     */
    @GetMapping("/address")
    @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackGetWarehouseAddress")
    @Retry(name = "warehouse")
    AddressDto getWarehouseAddress();
}