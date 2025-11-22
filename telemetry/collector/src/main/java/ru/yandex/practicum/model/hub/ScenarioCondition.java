package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Условие активации сценария.
 */
@Getter
@Setter
@ToString
public class ScenarioCondition {

    @NotBlank(message = "Идентификатор датчика не может быть пустым")
    private String sensorId;

    @NotNull(message = "Тип условия не может быть null")
    private ConditionType type;

    @NotNull(message = "Операция не может быть null")
    private ConditionOperation operation;

    @NotNull(message = "Значение не может быть null")
    private Integer value;
}