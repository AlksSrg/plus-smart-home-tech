package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;
import java.util.Map;

/**
 * Сервис для обработки снапшотов состояния сенсоров.
 * Проверяет выполнение условий сценариев на основе текущего состояния датчиков
 * и отправляет действия, если все условия выполнены.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotService {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient routerClient;

    /**
     * Обрабатывает снапшот состояния сенсоров.
     * Проверяет все сценарии для хаба и отправляет действия для выполненных сценариев.
     *
     * @param sensorsSnapshotAvro снапшот состояния сенсоров
     */
    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro sensorsSnapshotAvro) {
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshotAvro.getSensorsState();
        String hubId = sensorsSnapshotAvro.getHubId();

        log.info("Обработка снапшота для хаба: {}, сенсоров: {}",
                hubId, sensorStateMap.size());

        List<Scenario> scenariosList = scenarioRepository.findByHubId(hubId);
        log.debug("Найдено сценариев для хаба {}: {}", hubId, scenariosList.size());

        scenariosList.stream()
                .filter(scenario -> handleScenario(scenario, sensorStateMap))
                .forEach(scenario -> {
                    log.info("Отправка действий для сценария: {}", scenario.getName());
                    sendScenarioAction(scenario);
                });
    }

    private void sendScenarioAction(Scenario scenario) {
        List<ScenarioAction> scenarioActions = scenarioActionRepository.findByScenario(scenario);
        log.debug("Найдено действий для сценария {}: {}", scenario.getName(), scenarioActions.size());

        scenarioActions.forEach(action -> {
            try {
                routerClient.sendAction(action);
                log.debug("Действие успешно отправлено для сценария: {}", scenario.getName());
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария: {}", scenario.getName(), e);
            }
        });
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<ScenarioCondition> scenarioConditions = scenarioConditionRepository.findByScenario(scenario);
        log.debug("Проверка сценария: {}, условий: {}",
                scenario.getName(), scenarioConditions.size());

        if (scenarioConditions.isEmpty()) {
            log.debug("Сценарий {} не имеет условий, пропускаем", scenario.getName());
            return false;
        }

        boolean allConditionsMet = scenarioConditions.stream()
                .allMatch(sc -> checkCondition(sc.getCondition(), sc.getSensor().getId(), sensorStateMap));

        log.debug("Сценарий {}: все условия выполнены = {}", scenario.getName(), allConditionsMet);
        return allConditionsMet;
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        Integer targetValue = condition.getValue();
        return switch (condition.getOperation()) {
            case EQUALS -> targetValue.equals(currentValue);
            case GREATER_THAN -> currentValue > targetValue;
            case LOWER_THAN -> currentValue < targetValue;
        };
    }

    private boolean checkCondition(Condition condition, String sensorId, Map<String, SensorStateAvro> sensorStateMap) {
        SensorStateAvro sensorState = sensorStateMap.get(sensorId);
        if (sensorState == null) {
            log.debug("Датчик {} не найден в снапшоте", sensorId);
            return false;
        }

        try {
            return switch (condition.getType()) {
                case MOTION -> {
                    MotionSensorAvro motion = (MotionSensorAvro) sensorState.getData();
                    yield handleOperation(condition, motion.getMotion() ? 1 : 0);
                }
                case LUMINOSITY -> {
                    LightSensorAvro light = (LightSensorAvro) sensorState.getData();
                    yield handleOperation(condition, light.getLuminosity());
                }
                case SWITCH -> {
                    SwitchSensorAvro sw = (SwitchSensorAvro) sensorState.getData();
                    yield handleOperation(condition, sw.getState() ? 1 : 0);
                }
                case TEMPERATURE -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                    yield handleOperation(condition, climate.getTemperatureC());
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                    yield handleOperation(condition, climate.getCo2Level());
                }
                case HUMIDITY -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                    yield handleOperation(condition, climate.getHumidity());
                }
            };
        } catch (ClassCastException e) {
            log.error("Некорректный тип данных для датчика {}: {}", sensorId, e.getMessage());
            return false;
        }
    }
}