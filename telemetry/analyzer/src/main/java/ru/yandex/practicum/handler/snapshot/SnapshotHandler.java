package ru.yandex.practicum.handler.snapshot;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    public void handleSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.info("Обработка снапшота для хаба: {}", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.debug("Найдено {} сценариев для хаба: {}", scenarios.size(), hubId);

        for (Scenario scenario : scenarios) {
            try {
                // Проверяем условия сценария
                if (checkScenarioConditions(scenario, snapshot)) {
                    log.info("Условия сценария '{}' выполнены для хаба: {}", scenario.getName(), hubId);

                    // Выполняем действия сценария
                    executeScenarioActions(scenario, snapshot);
                }
            } catch (Exception e) {
                log.error("Ошибка при обработке сценария для хаба: {}", hubId, e);
            }
        }
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        List<ScenarioCondition> scenarioConditions = scenarioConditionRepository.findByScenario(scenario);

        if (scenarioConditions.isEmpty()) {
            log.debug("Сценарий '{}' не имеет условий", scenario.getName());
            return true;
        }

        Map<String, SensorStateAvro> sensorStates = snapshot.getSensorsState();

        for (ScenarioCondition scenarioCondition : scenarioConditions) {
            Condition condition = scenarioCondition.getCondition();
            Sensor sensor = scenarioCondition.getSensor();
            String sensorId = sensor.getId();

            // Проверяем, есть ли состояние для этого сенсора
            if (!sensorStates.containsKey(sensorId)) {
                log.debug("Состояние для сенсора {} не найдено в снапшоте", sensorId);
                return false;
            }

            SensorStateAvro sensorState = sensorStates.get(sensorId);

            // Находим значение датчика в состоянии
            int sensorValue = getSensorValueFromState(sensorState, condition.getType());

            // Проверяем условие
            if (!isConditionMet(condition, sensorValue)) {
                log.debug("Условие не выполнено для сенсора {}: значение {}, требуется {} {}",
                        sensorId, sensorValue, condition.getOperation(), condition.getValue());
                return false;
            }
        }

        log.debug("Все условия сценария '{}' выполнены", scenario.getName());
        return true;
    }

    private int getSensorValueFromState(SensorStateAvro sensorState, ConditionTypeAvro conditionType) {
        Object sensorData = sensorState.getData();

        switch (conditionType) {
            case TEMPERATURE:
                if (sensorData instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) sensorData).getTemperatureC();
                } else if (sensorData instanceof TemperatureSensorAvro) {
                    return ((TemperatureSensorAvro) sensorData).getTemperatureC();
                }
                break;
            case HUMIDITY:
                if (sensorData instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) sensorData).getHumidity();
                }
                break;
            case CO2LEVEL:
                if (sensorData instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) sensorData).getCo2Level();
                }
                break;
            case LUMINOSITY:
                if (sensorData instanceof LightSensorAvro) {
                    return ((LightSensorAvro) sensorData).getLuminosity();
                }
                break;
            case MOTION:
                if (sensorData instanceof MotionSensorAvro) {
                    return ((MotionSensorAvro) sensorData).getMotion() ? 1 : 0;
                }
                break;
            case SWITCH:
                if (sensorData instanceof SwitchSensorAvro) {
                    return ((SwitchSensorAvro) sensorData).getState() ? 1 : 0;
                }
                break;
        }

        log.warn("Неподдерживаемый тип данных для условия {}: {}", conditionType,
                sensorData != null ? sensorData.getClass() : "null");
        return 0;
    }

    private boolean isConditionMet(Condition condition, int sensorValue) {
        switch (condition.getOperation()) {
            case EQUALS:
                return sensorValue == condition.getValue();
            case GREATER_THAN:
                return sensorValue > condition.getValue();
            case LOWER_THAN:
                return sensorValue < condition.getValue();
            default:
                log.warn("Неподдерживаемая операция: {}", condition.getOperation());
                return false;
        }
    }

    private void executeScenarioActions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        List<ScenarioAction> scenarioActions = scenarioActionRepository.findByScenario(scenario);

        for (ScenarioAction scenarioAction : scenarioActions) {
            Action action = scenarioAction.getAction();
            Sensor sensor = scenarioAction.getSensor();

            try {
                sendDeviceAction(scenario, action, sensor, snapshot);
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сенсора {}", sensor.getId(), e);
            }
        }
    }

    private void sendDeviceAction(Scenario scenario, Action action, Sensor sensor, SensorsSnapshotAvro snapshot) {
        try {
            // Конвертируем ActionTypeAvro в ActionTypeProto
            ActionTypeProto actionTypeProto = mapActionType(action.getType());

            // Определяем значение для действия
            int actionValue = action.getValue() != null ? action.getValue() : 0;

            // Создаем DeviceActionProto
            DeviceActionProto deviceAction = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())  // Используем реальный ID устройства
                    .setType(actionTypeProto)
                    .setValue(actionValue)
                    .build();

            // Создаем timestamp
            Instant now = Instant.now();
            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

            // Создаем DeviceActionRequest согласно прото-схеме
            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(snapshot.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(deviceAction)
                    .setTimestamp(timestamp)
                    .build();

            hubRouterClient.sendRequest(request);
            log.info("Отправлено действие для сценария '{}', сенсор: {}, действие: {}, значение: {}",
                    scenario.getName(), sensor.getId(), action.getType(), actionValue);
        } catch (Exception e) {
            log.error("Ошибка при отправке команды для сенсора {}", sensor.getId(), e);
            throw e;
        }
    }

    private ActionTypeProto mapActionType(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
            default -> ActionTypeProto.UNRECOGNIZED;
        };
    }
}