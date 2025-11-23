package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Событие удаления сценария из системы.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioRemovedEvent extends HubEvent {

    /**
     * Название сценария.
     */
    @NotBlank(message = "Название сценария не может быть пустым")
    private String name;

    /**
     * Возвращает тип события хаба.
     *
     * @return тип события
     */
    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}