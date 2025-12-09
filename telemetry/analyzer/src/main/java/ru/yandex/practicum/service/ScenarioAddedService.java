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

    /**
     * @return имя payload-класса, который обслуживает данный сервис
     */
    @Override
    public String getPayloadType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие добавления сценария.
     *
     * @param hub событие хаба, содержащее ScenarioAddedEventAvro
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {

        ScenarioAddedEventAvro avro = (ScenarioAddedEventAvro) hub.getPayload();
        log.info("Добавление сценария '{}' для хаба {}", avro.getName(), hub.getHubId());

        Scenario scenario = scenarioRepository
                .findByHubIdAndName(hub.getHubId(), avro.getName())
                .orElseGet(() -> {
                    log.debug("Создаётся новый сценарий: {}", avro.getName());
                    return scenarioRepository.save(
                            Scenario.builder()
                                    .hubId(hub.getHubId())
                                    .name(avro.getName())
                                    .build()
                    );
                });

        // Обновляем имя, если оно изменилось
        if (!scenario.getName().equals(avro.getName())) {
            scenario.setName(avro.getName());
            scenarioRepository.save(scenario);
        }

        // Удаляем старые условия и действия
        scenarioConditionRepository.deleteByScenario(scenario);
        scenarioActionRepository.deleteByScenario(scenario);

        // Сохранение условий
        avro.getConditions().forEach(condAvro -> {
            Sensor sensor = getOrCreateSensor(condAvro.getSensorId(), hub.getHubId());
            Condition condition = saveCondition(condAvro);
            saveScenarioCondition(scenario, sensor, condition);
        });

        // Сохранение действий
        avro.getActions().forEach(actionAvro -> {
            Sensor sensor = getOrCreateSensor(actionAvro.getSensorId(), hub.getHubId());
            Action action = saveAction(actionAvro);
            saveScenarioAction(scenario, sensor, action);
        });

        log.info("Сценарий '{}' сохранён. Условий: {}. Действий: {}.",
                avro.getName(), avro.getConditions().size(), avro.getActions().size());
    }

    /**
     * Получает или создаёт новый датчик.
     *
     * @param sensorId ID датчика
     * @param hubId    ID хаба
     * @return существующий или новый Sensor
     */
    private Sensor getOrCreateSensor(String sensorId, String hubId) {
        return sensorRepository.findById(sensorId)
                .orElseGet(() ->
                        sensorRepository.save(
                                Sensor.builder()
                                        .id(sensorId)
                                        .hubId(hubId)
                                        .build()
                        )
                );
    }

    /**
     * Создаёт и сохраняет Condition из AVRO-объекта.
     *
     * @param conditionAvro условие из события
     * @return сохранённая сущность Condition
     */
    private Condition saveCondition(
            ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro conditionAvro) {

        return conditionRepository.save(
                Condition.builder()
                        .type(conditionAvro.getType())
                        .operation(conditionAvro.getOperation())
                        .value(asInteger(conditionAvro.getValue()))
                        .build()
        );
    }

    /**
     * Создаёт и сохраняет Action из AVRO-объекта.
     *
     * @param actionAvro действие из события
     * @return сохранённая сущность Action
     */
    private Action saveAction(DeviceActionAvro actionAvro) {
        return actionRepository.save(
                Action.builder()
                        .type(actionAvro.getType())
                        .value(actionAvro.getValue())
                        .build()
        );
    }

    /**
     * Сохраняет связь сценарий—датчик—условие.
     *
     * @param scenario  сценарий
     * @param sensor    датчик
     * @param condition условие
     */
    private void saveScenarioCondition(Scenario scenario,
                                       Sensor sensor,
                                       Condition condition) {
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

    /**
     * Сохраняет связь сценарий—датчик—действие.
     *
     * @param scenario сценарий
     * @param sensor   датчик
     * @param action   действие
     */
    private void saveScenarioAction(Scenario scenario,
                                    Sensor sensor,
                                    Action action) {

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

    /**
     * Приводит значение условия (Boolean/Integer) к Integer.
     *
     * @param value исходное значение из AVRO
     * @return число (0/1 или integer)
     */
    private Integer asInteger(Object value) {
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof Integer i) return i;
        log.warn("Неизвестный тип значения условия: {}", value);
        return 0;
    }
}
