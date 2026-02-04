package ru.yandex.practicum.dto.delivery;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.enums.DeliveryState;

import java.util.UUID;

/**
 * Data Transfer Object для представления доставки.
 * Используется для передачи данных о доставке между клиентом и сервером.
 */
@Data
public class DeliveryDto {

    /**
     * Уникальный идентификатор доставки.
     */
    @NotNull(message = "Идентификатор доставки обязателен")
    private UUID deliveryId;

    /**
     * Адрес отправления товара.
     */
    @NotNull(message = "Адрес отправления обязателен")
    private AddressDto fromAddress;

    /**
     * Адрес доставки товара.
     */
    @NotNull(message = "Адрес доставки обязателен")
    private AddressDto toAddress;

    /**
     * Идентификатор заказа, к которому относится доставка.
     */
    @NotNull(message = "Идентификатор заказа обязателен")
    private UUID orderId;

    /**
     * Текущий статус доставки.
     */
    @NotNull(message = "Статус доставки обязателен")
    private DeliveryState deliveryState;
}