package ru.yandex.practicum.feign;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

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
        public void addNewProduct(NewProductInWarehouseRequest request) {
            log.error("Резервный метод: Невозможно добавить новый товар на склад");
            throw new RuntimeException("Сервис склада недоступен: " + cause.getMessage());
        }

        @Override
        public BookedProductsDto checkProductQuantity(ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
            log.warn("Резервный метод: Проверка склада пропущена, разрешаем создание корзины");
            return BookedProductsDto.builder()
                    .deliveryWeight(0.0)
                    .deliveryVolume(0.0)
                    .fragile(false)
                    .build();
        }

        @Override
        public void addProductQuantity(AddProductToWarehouseRequest request) {
            log.error("Резервный метод: Невозможно добавить количество товара на склад");
            throw new RuntimeException("Сервис склада недоступен: " + cause.getMessage());
        }

        @Override
        public AddressDto getWarehouseAddress() {
            log.warn("Резервный метод: Возвращаем адрес склада по умолчанию");
            return AddressDto.builder()
                    .country("DEFAULT_ADDRESS")
                    .city("DEFAULT_ADDRESS")
                    .street("DEFAULT_ADDRESS")
                    .house("DEFAULT_ADDRESS")
                    .flat("DEFAULT_ADDRESS")
                    .build();
        }
    }
}