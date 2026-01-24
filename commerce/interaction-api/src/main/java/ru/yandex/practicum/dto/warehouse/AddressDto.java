package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO для представления адреса склада.
 * Используется для расчёта доставки.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddressDto {

    /**
     * Страна расположения склада.
     */
    private String country;

    /**
     * Город расположения склада.
     */
    private String city;

    /**
     * Улица расположения склада.
     */
    private String street;

    /**
     * Номер дома склада.
     */
    private String house;

    /**
     * Номер помещения/квартиры склада.
     */
    private String flat;
}