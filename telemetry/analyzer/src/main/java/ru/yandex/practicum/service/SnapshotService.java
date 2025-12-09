package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
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
     * Основной метод обработки снапшота.
     *
     * @param snapshot снапшот состояния датчиков
     */
    @Transactional
    public void handle(SensorsSnapshotAvro snapshot) {

        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> stateMap = snapshot.getSensorsState();

        log.info("Обработка снапшота: hubId={}, sensors={}", hubId, stateMap.size());

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        for (Scenario scenario : scenarios) {
            if (checkScenario(scenario, stateMap)) {
                sendScenarioActions(scenario);
            }
        }
    }

    /**
     * Проверяет выполнение всех условий сценария.
     *
     * @param scenario сценарий
     * @param stateMap карта состояний датчиков
     * @return true если все условия выполнены
     */
    private boolean checkScenario(Scenario scenario, Map<String, SensorStateAvro> stateMap) {

        List<ScenarioCondition> conditions =
                scenarioConditionRepository.findByScenario(scenario);

        if (conditions.isEmpty()) {
            log.debug("Сценарий {} не имеет условий — пропуск", scenario.getName());
            return false;
        }

        boolean result = conditions.stream()
                .allMatch(sc -> checkCondition(sc.getCondition(),
                        sc.getSensor().getId(),
                        stateMap));

        log.debug("Сценарий {}: условия выполнены = {}", scenario.getName(), result);

        return result;
    }

    /**
     * Проверяет одно условие сценария.
     *
     * @param condition условие
     * @param sensorId  ID датчика
     * @param stateMap  карта состояний датчиков
     * @return true если условие выполняется
     */
    private boolean checkCondition(Condition condition,
                                   String sensorId,
                                   Map<String, SensorStateAvro> stateMap) {

        SensorStateAvro state = stateMap.get(sensorId);
        if (state == null) {
            return false;
        }

        Object data = state.getData();
        if (data == null) {
            return false;
        }

        Integer currentValue;

        try {
            currentValue = switch (condition.getType()) {
                case MOTION -> {
                    MotionSensorAvro sensor = (MotionSensorAvro) data;
                    yield sensor.getMotion() ? 1 : 0;
                }
                case LUMINOSITY -> {
                    LightSensorAvro sensor = (LightSensorAvro) data;
                    yield sensor.getLuminosity();
                }
                case SWITCH -> {
                    SwitchSensorAvro sensor = (SwitchSensorAvro) data;
                    yield sensor.getState() ? 1 : 0;
                }
                case TEMPERATURE -> {
                    ClimateSensorAvro sensor = (ClimateSensorAvro) data;
                    yield sensor.getTemperatureC();
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro sensor = (ClimateSensorAvro) data;
                    yield sensor.getCo2Level();
                }
                case HUMIDITY -> {
                    ClimateSensorAvro sensor = (ClimateSensorAvro) data;
                    yield sensor.getHumidity();
                }
            };
        } catch (ClassCastException e) {
            log.error("Несоответствие типа датчика {}: {}", sensorId, e.getMessage());
            return false;
        }

        return switch (condition.getOperation()) {
            case EQUALS -> currentValue.equals(condition.getValue());
            case GREATER_THAN -> currentValue > condition.getValue();
            case LOWER_THAN -> currentValue < condition.getValue();
        };
    }

    /**
     * Отправляет все действия сценария в HubRouter.
     *
     * @param scenario сценарий
     */
    private void sendScenarioActions(Scenario scenario) {

        List<ScenarioAction> actions =
                scenarioActionRepository.findByScenario(scenario);

        for (ScenarioAction action : actions) {
            routerClient.sendAction(action);
        }

        log.info("Отправлено {} действий для сценария {}", actions.size(), scenario.getName());
    }
}
