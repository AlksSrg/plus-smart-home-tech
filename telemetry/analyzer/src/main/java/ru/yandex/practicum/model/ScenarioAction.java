package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность для связи сценария с действием и датчиком.
 * Представляет связь многие-ко-многим между Scenario, Sensor и Action
 * с использованием составного идентификатора.
 */
@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScenarioAction {

    @EmbeddedId
    private ScenarioActionId id;

    @MapsId("scenarioId")
    @JoinColumn(name = "scenario_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Scenario scenario;

    @MapsId("sensorId")
    @JoinColumn(name = "sensor_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Sensor sensor;

    @MapsId("actionId")
    @JoinColumn(name = "action_id")
    @ManyToOne(fetch = FetchType.LAZY)
    private Action action;
}