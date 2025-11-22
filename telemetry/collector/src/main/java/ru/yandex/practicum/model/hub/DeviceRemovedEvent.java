package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие удаления устройства из системы.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class DeviceRemovedEvent extends HubEvent {

    @NotBlank(message = "Идентификатор устройства не может быть пустым")
    private String id;

    /**
     * Возвращает тип события хаба.
     *
     * @return тип события
     */
    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED;
    }
}