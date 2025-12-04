package ru.yandex.practicum.handler.snapshot;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest; // ИЗМЕНИЛ ИМПОРТ
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {
    private final HubRouterClient hubRouterClient;
    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;

    @Transactional(readOnly = true)
    public void handleSnapshot(SensorsSnapshotAvro snapshot) {
        log.info("Обработка снапшота");
        String hubId = snapshot.getHubId();
        log.info("Поиск сценариев для хаба: {}", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        for (Scenario scenario : scenarios) {
            if (checkScenarioConditions(scenario, snapshot)) {
                executeScenarioActions(scenario);
            }
        }
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);
        for (Condition condition : conditions) {
            if (!checkCondition(condition, snapshot)) {
                log.info("Условие {} не выполнено для снапшота {}", condition, snapshot);
                return false;
            }
        }
        log.info("Сценарий '{}' активирован", scenario.getName());
        return true;
    }

    private void executeScenarioActions(Scenario scenario) {
        List<Action> actions = actionRepository.findAllByScenario(scenario);
        log.info("Отправка {} действий для хаба {}", actions.size(), scenario.getHubId());

        for (Action action : actions) {
            try {
                DeviceActionRequest request = DeviceActionRequest.newBuilder()
                        .setHubId(action.getScenario().getHubId())
                        .setScenarioName(action.getScenario().getName())
                        .setTimestamp(getCurrentTimestamp())
                        .setAction(DeviceActionProto.newBuilder()
                                .setSensorId(action.getSensor().getId())
                                .setType(ActionTypeProto.valueOf(action.getType().name()))
                                .setValue(action.getValue() == null ? 0 : action.getValue())
                                .build())
                        .build();
                log.info("Отправка запроса {}", request);
                hubRouterClient.sendRequest(request);
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария '{}'", scenario.getName(), e);
            }
        }
    }

    private Timestamp getCurrentTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    private boolean checkCondition(Condition condition, SensorsSnapshotAvro snapshot) {
        SensorStateAvro sensorState = snapshot.getSensorsState().get(condition.getSensor().getId());
        if (sensorState == null) {
            return false;
        }

        return switch (condition.getType()) {
            case MOTION -> {
                MotionSensorAvro motionSensorAvro = (MotionSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        motionSensorAvro.getMotion() ? 1 : 0);
            }
            case SWITCH -> {
                SwitchSensorAvro switchSensorAvro = (SwitchSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        switchSensorAvro.getState() ? 1 : 0);
            }
            case CO2LEVEL -> {
                ClimateSensorAvro climateSensorAvro = (ClimateSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        climateSensorAvro.getCo2Level());
            }
            case HUMIDITY -> {
                ClimateSensorAvro climateSensorAvro = (ClimateSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        climateSensorAvro.getHumidity());
            }
            case LUMINOSITY -> {
                LightSensorAvro lightSensorAvro = (LightSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        lightSensorAvro.getLuminosity());
            }
            case TEMPERATURE -> {
                ClimateSensorAvro climateSensorAvro = (ClimateSensorAvro) sensorState.getData();
                yield validateCondition(condition.getOperation(), condition.getValue(),
                        climateSensorAvro.getTemperatureC());
            }
            default -> false;
        };
    }

    private boolean validateCondition(ConditionOperationAvro operation, int conditionValue, int sensorValue) {
        switch (operation) {
            case EQUALS:
                return sensorValue == conditionValue;
            case LOWER_THAN:
                return sensorValue < conditionValue;
            case GREATER_THAN:
                return sensorValue > conditionValue;
            default:
                return false;
        }
    }
}