package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.*;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    @Override
    @Transactional
    public void handleEvent(HubEventAvro event) {
        ScenarioRemovedEventAvro scenarioRemovedEventAvro = (ScenarioRemovedEventAvro) event.getPayload();
        log.info("Получено событие удаления сценария: '{}' для хаба: {}",
                scenarioRemovedEventAvro.getName(), event.getHubId());

        scenarioRepository.findByHubIdAndName(event.getHubId(), scenarioRemovedEventAvro.getName())
                .ifPresentOrElse(
                        scenario -> {
                            // Находим все связи с действиями
                            List<ScenarioAction> scenarioActions = scenarioActionRepository.findByScenario(scenario);

                            // Удаляем каждое действие
                            for (ScenarioAction scenarioAction : scenarioActions) {
                                actionRepository.delete(scenarioAction.getAction());
                            }

                            // Находим все связи с условиями
                            List<ScenarioCondition> scenarioConditions = scenarioConditionRepository.findByScenario(scenario);

                            // Удаляем каждое условие
                            for (ScenarioCondition scenarioCondition : scenarioConditions) {
                                conditionRepository.delete(scenarioCondition.getCondition());
                            }

                            // Удаляем связи из промежуточных таблиц
                            scenarioActionRepository.deleteByScenario(scenario);
                            scenarioConditionRepository.deleteByScenario(scenario);

                            // Теперь можно удалить сценарий
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