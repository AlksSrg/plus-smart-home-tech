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
     * Возвращает тип полезной нагрузки, который обрабатывает этот сервис.
     *
     * @return имя класса полезной нагрузки
     */
    @Override
    public String getPayloadType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие добавления сценария.
     * Сохраняет сценарий, его условия и действия в базу данных.
     *
     * @param hub событие добавления сценария
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {
        ScenarioAddedEventAvro avro = (ScenarioAddedEventAvro) hub.getPayload();

        Scenario scenario = scenarioRepository
                .findByHubIdAndName(hub.getHubId(), avro.getName())
                .orElseGet(() -> scenarioRepository.save(
                        Scenario.builder()
                                .hubId(hub.getHubId())
                                .name(avro.getName())
                                .build()
                ));

        if (!scenario.getName().equals(avro.getName())) {
            scenario.setName(avro.getName());
            scenarioRepository.save(scenario);
        }

        scenarioConditionRepository.deleteByScenario(scenario);
        scenarioActionRepository.deleteByScenario(scenario);

        avro.getConditions().forEach(condAvro -> {
            Sensor sensor = getOrCreateSensor(condAvro.getSensorId(), hub.getHubId());
            Condition condition = saveCondition(condAvro);
            saveScenarioCondition(scenario, sensor, condition);
        });

        avro.getActions().forEach(actionAvro -> {
            Sensor sensor = getOrCreateSensor(actionAvro.getSensorId(), hub.getHubId());
            Action action = saveAction(actionAvro);
            saveScenarioAction(scenario, sensor, action);
        });

        log.info("Сценарий '{}' сохранён. Условий: {}. Действий: {}.",
                avro.getName(), avro.getConditions().size(), avro.getActions().size());
    }

    /**
     * Получает существующий датчик или создает новый.
     *
     * @param sensorId идентификатор датчика
     * @param hubId идентификатор хаба
     * @return найденный или созданный датчик
     */
    private Sensor getOrCreateSensor(String sensorId, String hubId) {
        return sensorRepository.findById(sensorId)
                .orElseGet(() -> sensorRepository.save(
                        Sensor.builder()
                                .id(sensorId)
                                .hubId(hubId)
                                .build()
                ));
    }

    /**
     * Сохраняет условие сценария в базу данных.
     *
     * @param conditionAvro условие в формате Avro
     * @return сохраненное условие
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
     * Сохраняет действие сценария в базу данных.
     *
     * @param actionAvro действие в формате Avro
     * @return сохраненное действие
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
     * Сохраняет связь между сценарием, датчиком и условием.
     *
     * @param scenario сценарий
     * @param sensor датчик
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
     * Сохраняет связь между сценарием, датчиком и действием.
     *
     * @param scenario сценарий
     * @param sensor датчик
     * @param action действие
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
     * Преобразует значение условия в целое число.
     *
     * @param value исходное значение
     * @return преобразованное целое число
     */
    private Integer asInteger(Object value) {
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof Integer i) return i;
        log.warn("Неизвестный тип значения условия: {}", value);
        return 0;
    }
}