package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.service.DeliveryService;

import java.util.UUID;

/**
 * Контроллер для управления доставками.
 * Предоставляет REST API для создания, отслеживания и управления статусами доставок.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController {

    private final DeliveryService deliveryService;

    /**
     * Создает новую доставку в системе.
     *
     * @param deliveryDto данные доставки для создания
     * @return созданная доставка с присвоенным идентификатором
     */
    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public DeliveryDto planDelivery(@RequestBody DeliveryDto deliveryDto) {
        log.info("PUT /api/v1/delivery - создание новой доставки для заказа: {}", deliveryDto.getOrderId());
        return deliveryService.createNewDelivery(deliveryDto);
    }

    /**
     * Отмечает доставку как успешно завершенную.
     *
     * @param deliveryId идентификатор доставки для отметки как успешной
     */
    @PostMapping("/successful")
    @ResponseStatus(HttpStatus.OK)
    public void deliverySuccessful(@RequestBody UUID deliveryId) {
        log.info("POST /api/v1/delivery/successful - успешная доставка: {}", deliveryId);
        deliveryService.changeStatusDeliveryOnDelivered(deliveryId);
    }

    /**
     * Отмечает товары как собранные и переданные в доставку.
     *
     * @param deliveryId идентификатор доставки для отметки как собранной
     */
    @PostMapping("/picked")
    @ResponseStatus(HttpStatus.OK)
    public void deliveryPicked(@RequestBody UUID deliveryId) {
        log.info("POST /api/v1/delivery/picked - товар получен для доставки: {}", deliveryId);
        deliveryService.pickedProductsOnDelivery(deliveryId);
    }

    /**
     * Отмечает доставку как неудачную.
     *
     * @param deliveryId идентификатор доставки для отметки как неудачной
     */
    @PostMapping("/failed")
    @ResponseStatus(HttpStatus.OK)
    public void deliveryFailed(@RequestBody UUID deliveryId) {
        log.info("POST /api/v1/delivery/failed - неудачная доставка: {}", deliveryId);
        deliveryService.changeStatusDeliveryOnFailed(deliveryId);
    }

    /**
     * Рассчитывает полную стоимость доставки для указанного заказа.
     *
     * @param orderDto данные заказа для расчета стоимости доставки
     * @return рассчитанная стоимость доставки
     */
    @PostMapping("/cost")
    @ResponseStatus(HttpStatus.OK)
    public Double deliveryCost(@RequestBody OrderDto orderDto) {
        log.info("POST /api/v1/delivery/cost - расчет стоимости доставки для заказа: {}", orderDto.getOrderId());
        return deliveryService.calculationCoastDelivery(orderDto);
    }
}