package ru.yandex.practicum.feign;

import feign.FeignException;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом заказов (Order).
 * Предоставляет методы для обновления статуса заказов.
 */
@FeignClient(name = "order")
public interface OrderFeignClient {

    /**
     * Обновляет статус заказа на "Оплачен".
     *
     * @param orderId идентификатор заказа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/api/v1/order/{orderId}/paid")
    void paymentOrder(@PathVariable UUID orderId) throws FeignException;

    /**
     * Обновляет статус заказа на "Ошибка оплаты".
     *
     * @param orderId идентификатор заказа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/api/v1/order/{orderId}/paymentFailed")
    void paymentOrderFailed(@PathVariable UUID orderId) throws FeignException;

    /**
     * Обновляет статус заказа на "Доставлен".
     *
     * @param orderId идентификатор заказа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/api/v1/order/{orderId}/delivered")
    void deliveryOrder(@PathVariable UUID orderId) throws FeignException;

    /**
     * Обновляет статус заказа на "Ошибка доставки".
     *
     * @param orderId идентификатор заказа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/api/v1/order/{orderId}/deliveryFailed")
    void deliveryOrderFailed(@PathVariable UUID orderId) throws FeignException;
}