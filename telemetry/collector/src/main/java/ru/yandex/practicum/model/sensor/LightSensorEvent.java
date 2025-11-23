package ru.yandex.practicum.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие датчика освещенности.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class LightSensorEvent extends SensorEvent {

    /**
     * Качество связи.
     */
    @NotNull(message = "Качество связи не может быть null")
    private int linkQuality;

    /**
     * Уровень освещенности.
     */
    @NotNull(message = "Освещенность не может быть null")
    private int luminosity;

    /**
     * Возвращает тип события датчика.
     *
     * @return тип события
     */
    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}