package ru.yandex.practicum.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие климатического датчика.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class ClimateSensorEvent extends SensorEvent {

    @NotNull(message = "Температура не может быть null")
    private int temperatureC;

    @NotNull(message = "Влажность не может быть null")
    private int humidity;

    @NotNull(message = "Уровень CO2 не может быть null")
    private int co2Level;

    /**
     * Возвращает тип события датчика.
     *
     * @return тип события
     */
    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}