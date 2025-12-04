package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;

    @Override
    public void handleEvent(HubEventAvro event) {
        ScenarioRemovedEventAvro scenarioRemovedEventAvro = (ScenarioRemovedEventAvro) event.getPayload();
        log.info("Получено событие удаления сценария: '{}' для хаба: {}",
                scenarioRemovedEventAvro.getName(), event.getHubId());

        scenarioRepository.findByHubIdAndName(event.getHubId(), scenarioRemovedEventAvro.getName())
                .ifPresentOrElse(
                        scenario -> {
                            actionRepository.deleteByScenario(scenario);
                            conditionRepository.deleteByScenario(scenario);
                            scenarioRepository.delete(scenario);
                            log.info("Сценарий '{}' успешно удален для хаба: {}",
                                    scenarioRemovedEventAvro.getName(), event.getHubId());
                        },
                        () -> log.info("Сценарий '{}' не найден для хаба: {}",
                                scenarioRemovedEventAvro.getName(), event.getHubId())
                );
    }

    @Override
    public String getEventType() {
        return ScenarioRemovedEventAvro.class.getSimpleName();
    }
}