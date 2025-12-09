package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.*;

/**
 * Сервис для обработки событий добавления сценариев.
 * Сохраняет новый сценарий с его условиями и действиями в базу данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedService implements HubEventService {

    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final SensorRepository sensorRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    @Override
    public String getPayloadType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие добавления сценария.
     * Сохраняет сценарий, его условия и действия в базу данных.
     * Если сценарий с таким именем уже существует - обновляет его.
     *
     * @param hub событие добавления сценария
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {
        ScenarioAddedEventAvro avro = (ScenarioAddedEventAvro) hub.getPayload();
        log.info("Добавление сценария с именем = {} для хаба = {}",
                avro.getName(), hub.getHubId());

        // Создаем или находим существующий сценарий
        Scenario scenario = scenarioRepository.findByHubIdAndName(hub.getHubId(), avro.getName())
                .orElseGet(() -> {
                    log.debug("Создание нового сценария: {}", avro.getName());
                    return scenarioRepository.save(
                            Scenario.builder()
                                    .hubId(hub.getHubId())
                                    .name(avro.getName())
                                    .build()
                    );
                });

        log.debug("Очистка существующих условий и действий для сценария: {}", scenario.getId());
        scenarioActionRepository.deleteByScenario(scenario);
        scenarioConditionRepository.deleteByScenario(scenario);

        // Сохраняем условия сценария
        log.debug("Сохранение условий сценария: {}", avro.getConditions().size());
        avro.getConditions().forEach(conditionAvro -> {
            Sensor sensor = getOrCreateSensor(conditionAvro.getSensorId(), hub.getHubId());
            Condition condition = saveCondition(conditionAvro);
            saveScenarioCondition(scenario, sensor, condition);
        });

        // Сохраняем действия сценария
        log.debug("Сохранение действий сценария: {}", avro.getActions().size());
        avro.getActions().forEach(actionAvro -> {
            Sensor sensor = getOrCreateSensor(actionAvro.getSensorId(), hub.getHubId());
            Action action = saveAction(actionAvro);
            saveScenarioAction(scenario, sensor, action);
        });

        log.info("Сценарий {} успешно сохранен с {} условиями и {} действиями",
                avro.getName(), avro.getConditions().size(), avro.getActions().size());
    }

    private Sensor getOrCreateSensor(String sensorId, String hubId) {
        return sensorRepository.findById(sensorId)
                .orElseGet(() -> {
                    log.debug("Создание нового датчика: {}", sensorId);
                    return sensorRepository.save(
                            Sensor.builder()
                                    .id(sensorId)
                                    .hubId(hubId)
                                    .build()
                    );
                });
    }

    private Condition saveCondition(ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro conditionAvro) {
        return conditionRepository.save(
                Condition.builder()
                        .type(conditionAvro.getType())
                        .operation(conditionAvro.getOperation())
                        .value(asInteger(conditionAvro.getValue()))
                        .build()
        );
    }

    private Action saveAction(DeviceActionAvro actionAvro) {
        return actionRepository.save(
                Action.builder()
                        .type(actionAvro.getType())
                        .value(actionAvro.getValue())
                        .build()
        );
    }

    private void saveScenarioCondition(Scenario scenario, Sensor sensor, Condition condition) {
        scenarioConditionRepository.save(
                ScenarioCondition.builder()
                        .scenario(scenario)
                        .sensor(sensor)
                        .condition(condition)
                        .id(new ScenarioConditionId(
                                scenario.getId(),
                                sensor.getId(),
                                condition.getId()))
                        .build()
        );
    }

    private void saveScenarioAction(Scenario scenario, Sensor sensor, Action action) {
        scenarioActionRepository.save(
                ScenarioAction.builder()
                        .scenario(scenario)
                        .sensor(sensor)
                        .action(action)
                        .id(new ScenarioActionId(
                                scenario.getId(),
                                sensor.getId(),
                                action.getId()))
                        .build()
        );
    }

    private Integer asInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            log.warn("Неизвестный тип значения условия: {}", value.getClass());
            return 0;
        }
    }
}