package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность сценария умного дома.
 * Определяет набор условий и действий для автоматизации.
 */
@Entity
@Table(name = "scenarios", uniqueConstraints = @UniqueConstraint(columnNames = {"hub_id", "name"}))
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Scenario {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;

    @Column(name = "name", nullable = false)
    private String name;
}