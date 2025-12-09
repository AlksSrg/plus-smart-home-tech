package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;

/**
 * Сущность условия сценария.
 * Определяет условие, которое должно быть выполнено для активации сценария.
 * Связь с Scenario осуществляется через сущность ScenarioCondition.
 */
@Entity
@Table(name = "conditions")
@Getter
@Setter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Condition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    Long id;

    @Column(name = "type")
    @Enumerated(EnumType.STRING)
    ConditionTypeAvro type;

    @Column(name = "operation")
    @Enumerated(EnumType.STRING)
    ConditionOperationAvro operation;

    @Column(name = "value")
    Integer value;
}