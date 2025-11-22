package ru.yandex.practicum.model.hub;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * Абстрактный базовый класс для событий хаба.
 * <p>
 * Определяет общие поля и механизм полиморфной десериализации.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        defaultImpl = HubEvent.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED")
})
@Getter
@Setter
@ToString
public abstract class HubEvent {

    @NotBlank(message = "Идентификатор хаба не может быть пустым")
    private String hubId;

    private Instant timestamp = Instant.now();

    /**
     * Возвращает тип конкретного события хаба.
     *
     * @return тип события
     */
    @NotNull
    public abstract HubEventType getType();
}