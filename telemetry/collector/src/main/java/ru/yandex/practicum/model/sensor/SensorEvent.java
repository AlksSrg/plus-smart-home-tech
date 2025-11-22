package ru.yandex.practicum.model.sensor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * Абстрактный базовый класс для событий датчиков.
 * <p>
 * Определяет общие поля и механизм полиморфной десериализации.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        defaultImpl = SensorEventType.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClimateSensorEvent.class, name = "CLIMATE_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = LightSensorEvent.class, name = "LIGHT_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = MotionSensorEvent.class, name = "MOTION_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = SwitchSensorEvent.class, name = "SWITCH_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = TemperatureSensorEvent.class, name = "TEMPERATURE_SENSOR_EVENT")
})
@Getter
@Setter
@ToString
public abstract class SensorEvent {

    @NotBlank(message = "Идентификатор датчика не может быть пустым")
    private String id;

    @NotBlank(message = "Идентификатор хаба не может быть пустым")
    private String hubId;

    private Instant timestamp = Instant.now();

    /**
     * Возвращает тип конкретного события датчика.
     *
     * @return тип события
     */
    public abstract SensorEventType getType();
}