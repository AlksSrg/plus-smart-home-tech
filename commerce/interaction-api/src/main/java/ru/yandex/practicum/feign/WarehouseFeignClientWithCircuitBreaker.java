package ru.yandex.practicum.feign;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.exception.order.ServiceUnavailableException;

import java.util.Map;
import java.util.UUID;

/**
 * Фабрика для создания резервной реализации Feign клиента при недоступности сервиса склада.
 * Реализует паттерн Circuit Breaker и Retry для повышения отказоустойчивости.
 */
@Slf4j
@Component
public class WarehouseFeignClientWithCircuitBreaker implements FallbackFactory<WarehouseFeignClient> {

    /**
     * Создает резервную реализацию Feign клиента.
     *
     * @param cause причина падения основного сервиса
     * @return резервная реализация клиента
     */
    @Override
    public WarehouseFeignClient create(Throwable cause) {
        return new WarehouseFeignClientFallback(cause);
    }

    /**
     * Резервная реализация Feign клиента для сервиса склада.
     */
    private static class WarehouseFeignClientFallback implements WarehouseFeignClient {
        private final Throwable cause;

        public WarehouseFeignClientFallback(Throwable cause) {
            this.cause = cause;
            log.error("Сервис склада недоступен, используется резервная реализация", cause);
        }

        @Override
        public BookedProductsDto checkQuantityProducts(ShoppingCartDto shoppingCartDto) {
            log.error("Сервис склада недоступен. Невозможно проверить доступность товаров для корзины");
            throw new ServiceUnavailableException("Нет заказываемого товара на складе");
        }

        @Override
        public void returnProductToWarehouse(Map<UUID, Integer> products) {
            log.error("Сервис склада недоступен. Невозможно вернуть товары на склад");
            throw new ServiceUnavailableException("Сервис склада недоступен");
        }

        @Override
        public void getProductOnOrderForDelivery(Map<UUID, Integer> products, UUID orderId) {
            log.error("Сервис склада недоступен. Невозможно зарезервировать товары для доставки заказа: {}", orderId);
            throw new ServiceUnavailableException("Сервис склада недоступен");
        }

        @Override
        public AddressDto getAddress() {
            log.error("Сервис склада недоступен. Невозможно получить адрес склада");
            throw new ServiceUnavailableException("Сервис склада недоступен");
        }
    }
}