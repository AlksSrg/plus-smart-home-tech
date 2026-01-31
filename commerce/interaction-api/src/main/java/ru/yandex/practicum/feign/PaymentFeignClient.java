package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.util.UUID;

/**
 * Feign клиент для взаимодействия с сервисом оплаты (Payment).
 * Предоставляет методы для работы с платежами.
 */
@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentFeignClient {

    /**
     * Создает платеж для заказа.
     *
     * @param orderDto информация о заказе
     * @return созданный платеж
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping
    PaymentDto makingPaymentForOrder(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    /**
     * Рассчитывает общую стоимость платежа.
     *
     * @param orderDto информация о заказе
     * @return общая стоимость
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/totalCost")
    Double calculateTotalCostPayment(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    /**
     * Подтверждает успешный платеж.
     *
     * @param paymentId идентификатор платежа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/refund")
    void successfulPayment(@Valid @RequestBody UUID paymentId) throws FeignException;

    /**
     * Рассчитывает стоимость товаров в заказе.
     *
     * @param orderDto информация о заказе
     * @return стоимость товаров
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/productCost")
    Double calculateProductCostPayment(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    /**
     * Обрабатывает неудачный платеж.
     *
     * @param paymentId идентификатор платежа
     * @throws FeignException при ошибке взаимодействия с сервисом
     */
    @PostMapping("/failed")
    void failedPayment(@RequestBody UUID paymentId) throws FeignException;
}