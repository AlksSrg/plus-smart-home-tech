package ru.yandex.practicum.feign;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

@Slf4j
@Component
public class WarehouseFeignClientWithCircuitBreaker implements FallbackFactory<WarehouseFeignClient> {

    @Override
    public WarehouseFeignClient create(Throwable cause) {
        return new WarehouseFeignClientFallback(cause);
    }

    private static class WarehouseFeignClientFallback implements WarehouseFeignClient {
        private final Throwable cause;

        public WarehouseFeignClientFallback(Throwable cause) {
            this.cause = cause;
            log.error("Warehouse service is unavailable, using fallback", cause);
        }

        @Override
        @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackAddNewProduct")
        @Retry(name = "warehouse")
        public void addNewProduct(NewProductInWarehouseRequest request) {
            throw new RuntimeException("Warehouse service unavailable: " + cause.getMessage());
        }

        @Override
        @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackCheckProductQuantity")
        @Retry(name = "warehouse")
        public BookedProductsDto checkProductQuantity(ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto) {
            log.warn("Warehouse service unavailable, allowing cart creation without quantity check");
            // Возвращаем успешный результат при недоступности склада
            return BookedProductsDto.builder()
                    .deliveryWeight(0.0)
                    .deliveryVolume(0.0)
                    .fragile(false)
                    .build();
        }

        @Override
        @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackAddProductQuantity")
        @Retry(name = "warehouse")
        public void addProductQuantity(AddProductToWarehouseRequest request) {
            throw new RuntimeException("Warehouse service unavailable: " + cause.getMessage());
        }

        @Override
        @CircuitBreaker(name = "warehouse", fallbackMethod = "fallbackGetWarehouseAddress")
        @Retry(name = "warehouse")
        public AddressDto getWarehouseAddress() {
            log.warn("Warehouse service unavailable, returning default address");
            return AddressDto.builder()
                    .country("DEFAULT_ADDRESS")
                    .city("DEFAULT_ADDRESS")
                    .street("DEFAULT_ADDRESS")
                    .house("DEFAULT_ADDRESS")
                    .flat("DEFAULT_ADDRESS")
                    .build();
        }

        // Fallback методы
        public void fallbackAddNewProduct(NewProductInWarehouseRequest request, Throwable t) {
            log.error("Fallback: Cannot add new product to warehouse", t);
            throw new RuntimeException("Warehouse service unavailable");
        }

        public BookedProductsDto fallbackCheckProductQuantity(
                ru.yandex.practicum.dto.cart.ShoppingCartDto shoppingCartDto, Throwable t) {
            log.warn("Fallback: Warehouse check skipped, allowing cart creation");
            return BookedProductsDto.builder()
                    .deliveryWeight(0.0)
                    .deliveryVolume(0.0)
                    .fragile(false)
                    .build();
        }

        public void fallbackAddProductQuantity(AddProductToWarehouseRequest request, Throwable t) {
            log.error("Fallback: Cannot add product quantity to warehouse", t);
            throw new RuntimeException("Warehouse service unavailable");
        }

        public AddressDto fallbackGetWarehouseAddress(Throwable t) {
            log.warn("Fallback: Returning default warehouse address");
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