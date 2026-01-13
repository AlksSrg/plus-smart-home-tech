package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;

/**
 * Составной идентификатор для сущности ScenarioAction.
 * Используется для представления связи многие-ко-многим между
 * Scenario, Sensor и Action с дополнительными атрибутами.
 */
@Embeddable
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class ScenarioActionId implements Serializable {

    @Column(name = "scenario_id")
    private Long scenarioId;

    @Column(name = "sensor_id")
    private String sensorId;

    @Column(name = "action_id")
    private Long actionId;
}