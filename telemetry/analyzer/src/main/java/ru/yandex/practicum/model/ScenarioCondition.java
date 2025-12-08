package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность для связи сценария с условием.
 * Представляет связь многие-ко-многим между Scenario и Condition.
 */
@Entity
@Table(name = "scenario_conditions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioCondition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @ManyToOne
    @JoinColumn(name = "condition_id")
    private Condition condition;

    @ManyToOne
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;
}