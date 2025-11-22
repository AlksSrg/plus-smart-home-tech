package ru.yandex.practicum.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие датчика температуры.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class TemperatureSensorEvent extends SensorEvent {

    @NotNull(message = "Температура в Цельсиях не может быть null")
    private Integer temperatureC;

    @NotNull(message = "Температура в Фаренгейтах не может быть null")
    private Integer temperatureF;

    /**
     * Возвращает тип события датчика.
     *
     * @return тип события
     */
    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}