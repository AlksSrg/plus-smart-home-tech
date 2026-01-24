package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO с информацией о товарах, доступных для бронирования.
 * Содержит параметры для расчёта доставки.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BookedProductsDto {

    /**
     * Общий вес товаров для доставки.
     */
    private Double deliveryWeight;

    /**
     * Общий объем товаров для доставки.
     */
    private Double deliveryVolume;

    /**
     * Признак наличия хрупких товаров в заказе.
     */
    private Boolean fragile;
}