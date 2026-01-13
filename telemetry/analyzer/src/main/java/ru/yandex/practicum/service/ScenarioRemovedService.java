package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.*;

import java.util.Optional;

/**
 * Сервис для обработки событий удаления сценариев.
 * Удаляет сценарий и все связанные с ним условия и действия из базы данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedService implements HubEventService {

    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public String getPayloadType() {
        return ScenarioRemovedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие удаления сценария.
     * Удаляет сценарий и все связанные с ним данные из базы данных.
     *
     * @param hub событие удаления сценария
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {
        ScenarioRemovedEventAvro scenarioRemovedEvent = (ScenarioRemovedEventAvro) hub.getPayload();
        log.info("Удаление сценария с именем = {} для хаба = {}",
                scenarioRemovedEvent.getName(), hub.getHubId());

        Optional<Scenario> scenarioOptional = scenarioRepository.findByHubIdAndName(
                hub.getHubId(), scenarioRemovedEvent.getName());

        if (scenarioOptional.isPresent()) {
            Scenario scenario = scenarioOptional.get();
            log.debug("Удаление связанных действий для сценария: {}", scenario.getId());
            scenarioActionRepository.deleteByScenario(scenario);

            log.debug("Удаление связанных условий для сценария: {}", scenario.getId());
            scenarioConditionRepository.deleteByScenario(scenario);

            log.debug("Удаление сценария: {}", scenario.getId());
            scenarioRepository.delete(scenario);

            log.info("Сценарий {} успешно удален", scenarioRemovedEvent.getName());
        } else {
            log.info("Сценарий с именем = {} для хаба = {} не найден",
                    scenarioRemovedEvent.getName(), hub.getHubId());
        }
    }
}