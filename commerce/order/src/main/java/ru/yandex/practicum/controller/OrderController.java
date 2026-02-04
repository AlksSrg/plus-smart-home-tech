package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.service.OrderService;

import java.util.UUID;

/**
 * REST контроллер для управления заказами.
 * Предоставляет API для работы с заказами в системе интернет-магазина.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Получить заказы пользователя.
     *
     * @param username имя пользователя
     * @param pageable параметры пагинации
     * @return список всех заказов пользователя (Точка улучшения и развития - пагинированный вывод)
     */
    @GetMapping
    public ResponseEntity<Page<OrderDto>> getClientOrders(
            @RequestParam String username,
            @PageableDefault(size = 10, page = 0, direction = Sort.Direction.DESC) Pageable pageable) {
        log.info("Method getClientOrders: username = {}", username);
        Page<OrderDto> orders = orderService.getOrderByUsername(username, pageable);
        return ResponseEntity.ok(orders);
    }

    /**
     * Создать новый заказ в системе.
     *
     * @param username    имя пользователя
     * @param createOrder запрос на создание заказа
     * @return оформленный заказ пользователя
     */
    @PutMapping
    public ResponseEntity<OrderDto> createNewOrder(
            @RequestParam String username,
            @Valid @RequestBody CreateNewOrderRequest createOrder) {
        log.info("Method createNewOrder: username = {}, order = {}", username, createOrder);
        OrderDto order = orderService.createNewOrder(username, createOrder);
        return ResponseEntity.ok(order);
    }

    /**
     * Возврат заказа.
     *
     * @param productReturn запрос на возврат заказа
     * @return заказ пользователя после сборки
     */
    @PostMapping("/return")
    public ResponseEntity<OrderDto> productReturn(@Valid @RequestBody ProductReturnRequest productReturn) {
        log.info("Method productReturn: ID = {}.", productReturn.getOrderId());
        OrderDto order = orderService.returnOrderProducts(productReturn);
        return ResponseEntity.ok(order);
    }

    /**
     * Оплата заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после оплаты
     */
    @PostMapping("/payment")
    public ResponseEntity<OrderDto> payment(@RequestBody UUID orderId) {
        log.info("Method payment: ID = {}.", orderId);
        OrderDto order = orderService.paymentOrder(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Оплата заказа произошла с ошибкой.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после ошибки оплаты
     */
    @PostMapping("/payment/failed")
    public ResponseEntity<OrderDto> paymentFailed(@RequestBody UUID orderId) {
        log.info("Method paymentFailed: orderId = {}.", orderId);
        OrderDto order = orderService.paymentOrderFailed(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Доставка заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после доставки
     */
    @PostMapping("/delivery")
    public ResponseEntity<OrderDto> delivery(@RequestBody UUID orderId) {
        log.info("Method delivery: orderId = {}.", orderId);
        OrderDto order = orderService.deliveryOrder(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Доставка заказа произошла с ошибкой.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после ошибки доставки
     */
    @PostMapping("/delivery/failed")
    public ResponseEntity<OrderDto> deliveryFailed(@RequestBody UUID orderId) {
        log.info("Method deliveryFailed: orderId = {}.", orderId);
        OrderDto order = orderService.deliveryOrderFailed(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Завершение заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после всех стадий и завершенный
     */
    @PostMapping("/completed")
    public ResponseEntity<OrderDto> complete(@RequestBody UUID orderId) {
        log.info("Method complete: orderId = {}.", orderId);
        OrderDto order = orderService.completedOrder(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Расчёт стоимости заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя с расчётом общей стоимости
     */
    @PostMapping("/calculate/total")
    public ResponseEntity<OrderDto> calculateTotalCost(@RequestBody UUID orderId) {
        log.info("Method calculateTotalCost: orderId = {}.", orderId);
        OrderDto order = orderService.calculateOrderTotalPrice(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Расчёт стоимости доставки заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя с расчётом доставки
     */
    @PostMapping("/calculate/delivery")
    public ResponseEntity<OrderDto> calculateDeliveryCost(@RequestBody UUID orderId) {
        log.info("Method calculateDeliveryCost: orderId = {}.", orderId);
        OrderDto order = orderService.calculateOrderDeliveryPrice(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Сборка заказа.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после сборки
     */
    @PostMapping("/assembly")
    public ResponseEntity<OrderDto> assembly(@RequestBody UUID orderId) {
        log.info("Method assembly: orderId = {}.", orderId);
        OrderDto order = orderService.assemblyOrder(orderId);
        return ResponseEntity.ok(order);
    }

    /**
     * Сборка заказа произошла с ошибкой.
     *
     * @param orderId идентификатор заказа
     * @return заказ пользователя после ошибки сборки
     */
    @PostMapping("/assembly/failed")
    public ResponseEntity<OrderDto> assemblyFailed(@RequestBody UUID orderId) {
        log.info("Method assemblyFailed: orderId = {}.", orderId);
        OrderDto order = orderService.assemblyOrderFailed(orderId);
        return ResponseEntity.ok(order);
    }
}