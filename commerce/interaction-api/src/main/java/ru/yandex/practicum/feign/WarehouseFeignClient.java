package ru.yandex.practicum.feign;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;

import java.util.Map;
import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом склада.
 * Определяет REST-эндпоинты для вызова операций склада.
 * Класс включает аннотации Resilience4j для обеспечения отказоустойчивости:
 * CircuitBreaker для предотвращения каскадных сбоев и Retry для повторных попыток.
 * При недоступности основного сервиса используется резервная реализация через fallbackFactory.
 */
@FeignClient(
        name = "warehouse",
        path = "/api/v1/warehouse",
        configuration = FeignClientConfig.class,
        fallbackFactory = WarehouseFeignClientWithCircuitBreaker.class
)
public interface WarehouseFeignClient {

    /**
     * Проверяет доступность товаров на складе для корзины покупок.
     * Метод выполняет проверку наличия товаров в требуемом количестве
     * и возвращает информацию о доступных товарах.
     *
     * @param shoppingCartDto данные корзины покупок, содержащие товары и их количество
     * @return DTO с информацией о доступных товарах и их количестве
     */
    @PostMapping("/check")
    @CircuitBreaker(name = "warehouse-service")
    @Retry(name = "warehouse-service")
    BookedProductsDto checkQuantityProducts(@RequestBody ShoppingCartDto shoppingCartDto);

    /**
     * Возвращает товары на склад.
     * Используется для возврата товаров при отмене заказов
     * или других операциях, требующих возврата товаров на склад.
     *
     * @param products товары для возврата в формате (идентификатор товара -> количество)
     */
    @PostMapping("/return")
    @CircuitBreaker(name = "warehouse-service")
    @Retry(name = "warehouse-service")
    void returnProductToWarehouse(@RequestBody Map<UUID, Integer> products);

    /**
     * Резервирует товары для доставки заказа.
     * Метод резервирует указанные товары в нужном количестве
     * для конкретного заказа, подготавливая их к отгрузке.
     *
     * @param products товары для резервирования в формате (идентификатор товара -> количество)
     * @param orderId  идентификатор заказа, для которого резервируются товары
     */
    @PostMapping("/ship/order")
    @CircuitBreaker(name = "warehouse-service")
    @Retry(name = "warehouse-service")
    void getProductOnOrderForDelivery(@RequestBody Map<UUID, Integer> products, @RequestParam UUID orderId);

    /**
     * Получает адрес склада.
     * Возвращает информацию о физическом местоположении склада,
     * используемую для расчетов доставки и логистики.
     *
     * @return DTO с адресом склада, включая город, улицу, дом и другие детали
     */
    @GetMapping("/address")
    @CircuitBreaker(name = "warehouse-service")
    @Retry(name = "warehouse-service")
    AddressDto getAddress();
}