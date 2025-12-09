package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
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
 * Сервис обработки снапшота состояний датчиков.
 * Проверяет выполнение сценариев и отправляет команды в HubRouter.
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
     * Обрабатывает снапшот сенсоров.
     *
     * @param snapshot агрегированный снимок данных
     */
    @Transactional
    public void handle(SensorsSnapshotAvro snapshot) {

        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> stateMap = snapshot.getSensorsState();

        log.info("Обработка снапшота: hubId={}, количество сенсоров={}", hubId, stateMap.size());

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        for (Scenario scenario : scenarios) {
            if (checkScenario(scenario, stateMap)) {
                sendScenarioActions(scenario);
            }
        }
    }

    /**
     * Проверяет выполнение условий сценария.
     */
    private boolean checkScenario(Scenario scenario, Map<String, SensorStateAvro> stateMap) {

        List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);

        if (conditions.isEmpty()) {
            log.debug("Сценарий '{}' не содержит условий — пропуск", scenario.getName());
            return false;
        }

        boolean ok = conditions.stream()
                .allMatch(sc -> checkCondition(
                        sc.getCondition(),
                        sc.getSensor().getId(),
                        stateMap
                ));

        log.debug("Сценарий '{}': выполнение условий = {}", scenario.getName(), ok);

        return ok;
    }

    /**
     * Проверяет выполнение одного условия.
     */
    private boolean checkCondition(Condition condition,
                                   String sensorId,
                                   Map<String, SensorStateAvro> stateMap) {

        SensorStateAvro state = stateMap.get(sensorId);
        if (state == null || state.getData() == null) {
            return false;
        }

        Object data = state.getData();
        Integer currentValue;

        try {
            currentValue = switch (condition.getType()) {
                case MOTION -> ((MotionSensorAvro) data).getMotion() ? 1 : 0;
                case LUMINOSITY -> ((LightSensorAvro) data).getLuminosity();
                case SWITCH -> ((SwitchSensorAvro) data).getState() ? 1 : 0;
                case TEMPERATURE -> ((ClimateSensorAvro) data).getTemperatureC();
                case CO2LEVEL -> ((ClimateSensorAvro) data).getCo2Level();
                case HUMIDITY -> ((ClimateSensorAvro) data).getHumidity();
            };
        } catch (ClassCastException e) {
            log.error("Ошибка типа данных датчика '{}': {}", sensorId, e.getMessage());
            return false;
        }

        return switch (condition.getOperation()) {
            case EQUALS -> currentValue.equals(condition.getValue());
            case GREATER_THAN -> currentValue > condition.getValue();
            case LOWER_THAN -> currentValue < condition.getValue();
        };
    }

    /**
     * Отправляет действия сценария в HubRouter.
     *
     * @param scenario сценарий
     */
    private void sendScenarioActions(Scenario scenario) {

        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

        for (ScenarioAction actionLink : actions) {

            String sensorId = actionLink.getSensor().getId();
            var action = actionLink.getAction();

            DeviceActionProto.Builder protoBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sensorId);

            if (action.getType() != null) {
                try {
                    protoBuilder.setType(ActionTypeProto.valueOf(action.getType().name()));
                } catch (IllegalArgumentException e) {
                    log.warn("Неподдерживаемый тип действия '{}', используем значение по умолчанию", action.getType());
                }
            }

            // Значение — если есть
            if (action.getValue() != null) {
                protoBuilder.setValue(action.getValue());
            }

            DeviceActionProto proto = protoBuilder.build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(proto)
                    .setTimestamp(Timestamp.getDefaultInstance())
                    .build();

            routerClient.sendAction(request);
        }

        log.info("Отправлено {} действий для сценария '{}'", actions.size(), scenario.getName());
    }
}
