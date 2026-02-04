package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом доставки.
 * Определяет контракт для вызова API сервиса доставки.
 */
@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryFeignClient {

    /**
     * Создает новую доставку.
     *
     * @param deliveryDto данные для создания доставки
     * @return созданная доставка
     * @throws FeignException если произошла ошибка при вызове API
     */
    @PutMapping
    DeliveryDto createNewDelivery(@Valid @RequestBody DeliveryDto deliveryDto) throws FeignException;

    /**
     * Отмечает доставку как успешно завершенную.
     *
     * @param deliveryId идентификатор доставки
     * @throws FeignException если произошла ошибка при вызове API
     */
    @PostMapping("/successful")
    void changeStatusDeliveryOnDelivered(@Valid @RequestBody UUID deliveryId) throws FeignException;

    /**
     * Помечает товары как собранные для доставки.
     *
     * @param deliveryId идентификатор доставки
     * @throws FeignException если произошла ошибка при вызове API
     */
    @PostMapping("/picked")
    void pickedProductsOnDelivery(@Valid @RequestBody UUID deliveryId) throws FeignException;

    /**
     * Отмечает доставку как неудачную.
     *
     * @param deliveryId идентификатор доставки
     * @throws FeignException если произошла ошибка при вызове API
     */
    @PostMapping("/failed")
    void changeStatusDeliveryOnFailed(@Valid @RequestBody UUID deliveryId) throws FeignException;

    /**
     * Рассчитывает стоимость доставки для заказа.
     *
     * @param orderDto данные заказа для расчета
     * @return стоимость доставки
     * @throws FeignException если произошла ошибка при вызове API
     */
    @PostMapping("/cost")
    Double calculationCoastDelivery(@Valid @RequestBody OrderDto orderDto) throws FeignException;
}