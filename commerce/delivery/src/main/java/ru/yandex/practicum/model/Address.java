package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

/**
 * Сущность для представления адреса в базе данных.
 * Хранит информацию о географическом адресе.
 */
@Entity
@Table(name = "addresses")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Address {

    /**
     * Уникальный идентификатор адреса.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id")
    private UUID id;

    /**
     * Название страны.
     */
    @Column(name = "country", nullable = false)
    private String country;

    /**
     * Название города.
     */
    @Column(name = "city", nullable = false)
    private String city;

    /**
     * Название улицы.
     */
    @Column(name = "street", nullable = false)
    private String street;

    /**
     * Номер дома.
     */
    @Column(name = "house", nullable = false)
    private String house;

    /**
     * Номер квартиры (опционально).
     */
    @Column(name = "flat")
    private String flat;

    /**
     * Возвращает строковое представление адреса.
     *
     * @return форматированная строка адреса
     */
    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s",
                street, house, city, country, flat != null ? "кв. " + flat : "");
    }
}