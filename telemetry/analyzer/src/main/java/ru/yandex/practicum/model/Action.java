package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;

/**
 * Сущность действия сценария.
 * Определяет действие, которое должно быть выполнено при активации сценария.
 * Связь с Scenario осуществляется через сущность ScenarioAction.
 */
@Entity
@Table(name = "actions")
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Action {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "type")
    @Enumerated(EnumType.STRING)
    ActionTypeAvro type;

    @Column(name = "value")
    Integer value;
}