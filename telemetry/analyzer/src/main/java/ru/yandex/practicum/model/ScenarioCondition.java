package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность для связи сценария с условием и датчиком.
 * Представляет связь многие-ко-многим между Scenario, Sensor и Condition
 * с использованием составного идентификатора.
 */
@Entity
@Table(name = "scenario_conditions")
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScenarioCondition {

    @EmbeddedId
    private ScenarioConditionId id;

    @MapsId("scenarioId")
    @JoinColumn(name = "scenario_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Scenario scenario;

    @MapsId("sensorId")
    @JoinColumn(name = "sensor_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Sensor sensor;

    @MapsId("conditionId")
    @JoinColumn(name = "condition_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Condition condition;
}