package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;

@Entity
@Table(name = "conditions")
@SecondaryTable(name = "scenario_conditions", pkJoinColumns = @PrimaryKeyJoinColumn(name = "condition_id"))
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
    int value;

    @ManyToOne
    @JoinColumn(table = "scenario_conditions", name = "scenario_id")
    Scenario scenario;

    @ManyToOne
    @JoinColumn(table = "scenario_conditions", name = "sensor_id")
    Sensor sensor;
}