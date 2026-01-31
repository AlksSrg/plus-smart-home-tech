package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Представление адреса в системе.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddressDto {

    /**
     * Страна.
     */
    private String country;

    /**
     * Город.
     */
    private String city;

    /**
     * Улица.
     */
    private String street;

    /**
     * Дом.
     */
    private String house;

    /**
     * Квартира.
     */
    private String flat;
}