package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.service.PaymentService;

import java.util.UUID;

/**
 * Контроллер для обработки HTTP-запросов, связанных с платежами.
 * Предоставляет REST API для создания и управления платежами.
 */
@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/payment")
public class PaymentController {

    private final PaymentService paymentService;

    /**
     * Создает платеж для заказа.
     *
     * @param orderDto информация о заказе
     * @return созданный платеж
     */
    @PostMapping
    public PaymentDto makingPaymentForOrder(@Valid @RequestBody OrderDto orderDto) {
        log.info("Method makingPaymentForOrder: orderDto = {}", orderDto);
        return paymentService.makingPaymentForOrder(orderDto);
    }

    /**
     * Рассчитывает общую стоимость платежа.
     *
     * @param orderDto информация о заказе
     * @return общая стоимость
     */
    @PostMapping("/totalCost")
    public Double calculateTotalCostPayment(@Valid @RequestBody OrderDto orderDto) {
        log.info("Method calculateTotalCostPayment: ID = {}", orderDto.getOrderId());
        return paymentService.calculateTotalCostPayment(orderDto);
    }

    /**
     * Подтверждает успешный платеж.
     *
     * @param paymentId идентификатор платежа
     */
    @PostMapping("/refund")
    public void successfulPayment(@Valid @RequestBody UUID paymentId) {
        log.info("Method successfulPayment: paymentID = {}", paymentId);
        paymentService.successfulPayment(paymentId);
    }

    /**
     * Рассчитывает стоимость товаров в заказе.
     *
     * @param orderDto информация о заказе
     * @return стоимость товаров
     */
    @PostMapping("/productCost")
    public Double calculateProductCostPayment(@Valid @RequestBody OrderDto orderDto) {
        log.info("Method calculateProductCostPayment: ID = {}", orderDto.getOrderId());
        return paymentService.calculateProductCostPayment(orderDto);
    }

    /**
     * Обрабатывает неудачный платеж.
     *
     * @param paymentId идентификатор платежа
     */
    @PostMapping("/failed")
    public void failedPayment(@RequestBody UUID paymentId) {
        log.info("Method failedPayment: paymentId {}.", paymentId);
        paymentService.failedPayment(paymentId);
    }
}