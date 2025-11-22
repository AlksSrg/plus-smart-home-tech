package ru.yandex.practicum.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие датчика движения.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class MotionSensorEvent extends SensorEvent {

    @NotNull(message = "Качество связи не может быть null")
    private Integer linkQuality;

    @NotNull(message = "Обнаружение движения не может быть null")
    private Boolean motion;

    @NotNull(message = "Напряжение не может быть null")
    private Integer voltage;

    /**
     * Возвращает тип события датчика.
     *
     * @return тип события
     */
    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}