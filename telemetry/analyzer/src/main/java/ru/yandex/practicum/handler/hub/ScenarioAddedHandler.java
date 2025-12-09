package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Обработчик событий добавления нового сценария.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    @Transactional
    public void handleEvent(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAddedEventAvro = (ScenarioAddedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String scenarioName = scenarioAddedEventAvro.getName();

        log.info("Получено событие добавления сценария: '{}' для хаба: {}", scenarioName, hubId);

        // Собираем все ID датчиков
        Set<String> allSensorIds = collectAllSensorIds(scenarioAddedEventAvro);

        // Проверяем существование всех датчиков
        if (!sensorRepository.existsByIdInAndHubId(allSensorIds, hubId, allSensorIds.size())) {
            log.error("Не найдены некоторые датчики для хаба: {}", hubId);
            return;
        }

        // Находим или создаем сценарий
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .orElseGet(() -> {
                    Scenario newScenario = new Scenario();
                    newScenario.setHubId(hubId);
                    newScenario.setName(scenarioName);
                    return scenarioRepository.save(newScenario);
                });

        // Удаляем старые связи
        scenarioConditionRepository.deleteByScenario(scenario);
        scenarioActionRepository.deleteByScenario(scenario);

        // Сохраняем условия
        saveConditions(scenarioAddedEventAvro.getConditions(), scenario);

        // Сохраняем действия
        saveActions(scenarioAddedEventAvro.getActions(), scenario);

        log.info("Сценарий '{}' успешно обновлен для хаба: {}", scenarioName, hubId);
    }

    @Override
    public String getEventType() {
        return "ScenarioAddedEventAvro";
    }

    private Set<String> collectAllSensorIds(ScenarioAddedEventAvro scenarioAddedEventAvro) {
        Set<String> sensorIds = scenarioAddedEventAvro.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .collect(Collectors.toSet());

        scenarioAddedEventAvro.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .forEach(sensorIds::add);

        return sensorIds;
    }

    private void saveConditions(List<ScenarioConditionAvro> conditionsAvro, Scenario scenario) {
        List<Condition> conditions = new ArrayList<>();
        List<ScenarioCondition> scenarioConditions = new ArrayList<>();

        for (ScenarioConditionAvro conditionAvro : conditionsAvro) {
            // Создаем условие
            Condition condition = new Condition();
            condition.setType(conditionAvro.getType());
            condition.setOperation(conditionAvro.getOperation());
            condition.setValue(mapConditionValue(conditionAvro.getValue()));

            Condition savedCondition = conditionRepository.save(condition);
            conditions.add(savedCondition);

            // Находим датчик
            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Датчик не найден: " + conditionAvro.getSensorId()));

            // Создаем связь
            ScenarioCondition scenarioCondition = new ScenarioCondition();
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setCondition(savedCondition);
            scenarioCondition.setSensor(sensor);
            scenarioConditions.add(scenarioCondition);
        }

        scenarioConditionRepository.saveAll(scenarioConditions);
        log.info("Сохранено {} условий для сценария: '{}'", conditions.size(), scenario.getName());
    }

    private void saveActions(List<DeviceActionAvro> actionsAvro, Scenario scenario) {
        List<Action> actions = new ArrayList<>();
        List<ScenarioAction> scenarioActions = new ArrayList<>();

        for (DeviceActionAvro actionAvro : actionsAvro) {
            // Создаем действие
            Action action = new Action();
            action.setType(actionAvro.getType());
            action.setValue(actionAvro.getValue() != null ? actionAvro.getValue() : 0);

            Action savedAction = actionRepository.save(action);
            actions.add(savedAction);

            // Находим датчик
            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Датчик не найден: " + actionAvro.getSensorId()));

            // Создаем связь
            ScenarioAction scenarioAction = new ScenarioAction();
            scenarioAction.setScenario(scenario);
            scenarioAction.setAction(savedAction);
            scenarioAction.setSensor(sensor);
            scenarioActions.add(scenarioAction);
        }

        scenarioActionRepository.saveAll(scenarioActions);
        log.info("Сохранено {} действий для сценария: '{}'", actions.size(), scenario.getName());
    }

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