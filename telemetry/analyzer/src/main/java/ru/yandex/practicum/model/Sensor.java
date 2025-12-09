package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность датчика (устройства).
 * Представляет датчик, подключенный к хабу умного дома.
 */
@Entity
@Table(name = "sensors")
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;
}