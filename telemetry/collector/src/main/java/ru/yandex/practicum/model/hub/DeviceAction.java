package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Действие, выполняемое устройством в рамках сценария.
 */
@Getter
@Setter
@ToString
public class DeviceAction {

    /**
     * Идентификатор датчика.
     */
    @NotBlank(message = "Идентификатор датчика не может быть пустым")
    private String sensorId;

    /**
     * Тип действия.
     */
    @NotNull(message = "Тип действия не может быть null")
    private ActionType type;

    /**
     * Значение действия.
     */
    private Integer value;
}