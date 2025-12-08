package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Сущность для связи сценария с действием.
 * Представляет связь многие-ко-многим между Scenario и Action.
 */
@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioAction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @ManyToOne
    @JoinColumn(name = "action_id")
    private Action action;

    @ManyToOne
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;
}