package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.*;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Обработчик событий добавления нового сценария.
 * Сохраняет сценарий с его условиями и действиями в базе данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;

    /**
     * Обрабатывает событие добавления сценария.
     * Сохраняет или обновляет сценарий с его условиями и действиями.
     *
     * @param event событие добавления сценария
     */
    @Override
    @Transactional
    public void handleEvent(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAddedEventAvro = (ScenarioAddedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String scenarioName = scenarioAddedEventAvro.getName();

        log.info("Получено событие добавления сценария: '{}' для хаба: {}", scenarioName, hubId);

        // Собираем все ID датчиков из условий и действий
        Set<String> allSensorIds = collectAllSensorIds(scenarioAddedEventAvro);

        // Проверяем существование всех датчиков
        if (!sensorRepository.existsByIdInAndHubId(allSensorIds, hubId)) {
            log.error("Не найдены некоторые датчики для хаба: {}", hubId);
            return;
        }

        // Находим или создаем сценарий
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .orElseGet(() -> createNewScenario(hubId, scenarioName));

        // Удаляем старые условия и действия (для обновления сценария)
        conditionRepository.deleteByScenario(scenario);
        actionRepository.deleteByScenario(scenario);

        // Сохраняем новые условия
        List<Condition> conditions = mapToConditions(scenarioAddedEventAvro.getConditions(), scenario);
        if (!conditions.isEmpty()) {
            conditionRepository.saveAll(conditions);
            log.info("Сохранено {} условий для сценария: '{}'", conditions.size(), scenarioName);
        }

        // Сохраняем новые действия
        List<Action> actions = mapToActions(scenarioAddedEventAvro.getActions(), scenario);
        if (!actions.isEmpty()) {
            actionRepository.saveAll(actions);
            log.info("Сохранено {} действий для сценария: '{}'", actions.size(), scenarioName);
        }

        log.info("Сценарий '{}' успешно обновлен для хаба: {}", scenarioName, hubId);
    }

    /**
     * Возвращает тип обрабатываемого события.
     *
     * @return тип события "ScenarioAddedEventAvro"
     */
    @Override
    public String getEventType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    /**
     * Собирает все идентификаторы датчиков из условий и действий сценария.
     *
     * @param scenarioAddedEventAvro событие добавления сценария
     * @return набор идентификаторов датчиков
     */
    private Set<String> collectAllSensorIds(ScenarioAddedEventAvro scenarioAddedEventAvro) {
        Set<String> sensorIds = scenarioAddedEventAvro.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .collect(Collectors.toSet());

        scenarioAddedEventAvro.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .forEach(sensorIds::add);

        return sensorIds;
    }

    /**
     * Создает новый сценарий.
     *
     * @param hubId        идентификатор хаба
     * @param scenarioName название сценария
     * @return созданный сценарий
     */
    private Scenario createNewScenario(String hubId, String scenarioName) {
        Scenario newScenario = Scenario.builder()
                .name(scenarioName)
                .hubId(hubId)
                .build();
        return scenarioRepository.save(newScenario);
    }

    /**
     * Преобразует условия из Avro формата в сущности Condition.
     *
     * @param conditionsAvro условия в формате Avro
     * @param scenario       сценарий, к которому относятся условия
     * @return список сущностей Condition
     */
    private List<Condition> mapToConditions(List<ScenarioConditionAvro> conditionsAvro, Scenario scenario) {
        return conditionsAvro.stream()
                .map(conditionAvro -> Condition.builder()
                        .sensor(sensorRepository.findById(conditionAvro.getSensorId())
                                .orElseThrow(() -> new IllegalArgumentException(
                                        "Датчик не найден: " + conditionAvro.getSensorId())))
                        .operation(conditionAvro.getOperation())
                        .scenario(scenario)
                        .type(conditionAvro.getType())
                        .value(mapConditionValue(conditionAvro.getValue()))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Преобразует действия из Avro формата в сущности Action.
     *
     * @param actionsAvro действия в формате Avro
     * @param scenario    сценарий, к которому относятся действия
     * @return список сущностей Action
     */
    private List<Action> mapToActions(List<DeviceActionAvro> actionsAvro, Scenario scenario) {
        return actionsAvro.stream()
                .map(actionAvro -> Action.builder()
                        .sensor(sensorRepository.findById(actionAvro.getSensorId())
                                .orElseThrow(() -> new IllegalArgumentException(
                                        "Датчик не найден: " + actionAvro.getSensorId())))
                        .scenario(scenario)
                        .type(actionAvro.getType())
                        .value(actionAvro.getValue() == null ? 0 : actionAvro.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Преобразует значение условия в целое число.
     * Поддерживает Integer и Boolean типы.
     *
     * @param value значение условия
     * @return целочисленное значение
     * @throws IllegalArgumentException если тип значения не поддерживается
     */
    private int mapConditionValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            throw new IllegalArgumentException("Неподдерживаемый тип значения условия: " + value.getClass());
        }
    }
}