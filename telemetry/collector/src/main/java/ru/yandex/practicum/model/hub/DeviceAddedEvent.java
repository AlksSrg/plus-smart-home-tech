package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие добавления устройства в систему.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class DeviceAddedEvent extends HubEvent {

    /**
     * Идентификатор устройства.
     */
    @NotBlank(message = "Идентификатор устройства не может быть пустым")
    private String id;

    /**
     * Тип устройства.
     */
    @NotNull(message = "Тип устройства не может быть null")
    private DeviceType deviceType;

    /**
     * Возвращает тип события хаба.
     *
     * @return тип события
     */
    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}