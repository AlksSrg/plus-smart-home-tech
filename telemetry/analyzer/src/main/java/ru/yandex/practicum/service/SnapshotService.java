package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
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
            log.warn("Датчик '{}' не найден в снапшоте или данные отсутствуют", sensorId);
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

        boolean result = switch (condition.getOperation()) {
            case EQUALS -> currentValue.equals(condition.getValue());
            case GREATER_THAN -> currentValue > condition.getValue();
            case LOWER_THAN -> currentValue < condition.getValue();
        };

        log.debug("Проверка условия для датчика '{}': текущее значение={}, требуемое={}, операция={}, результат={}",
                sensorId, currentValue, condition.getValue(), condition.getOperation(), result);

        return result;
    }

    /**
     * Отправляет действия сценария в HubRouter.
     *
     * @param scenario сценарий
     */
    private void sendScenarioActions(Scenario scenario) {

        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

        if (actions.isEmpty()) {
            log.warn("Сценарий '{}' не содержит действий", scenario.getName());
            return;
        }

        log.info("Отправка действий для сценария '{}': найдено {} действий", scenario.getName(), actions.size());

        for (ScenarioAction actionLink : actions) {

            String sensorId = actionLink.getSensor().getId();
            var action = actionLink.getAction();

            DeviceActionProto.Builder protoBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sensorId);

            if (action.getType() != null) {
                try {
                    ActionTypeProto actionType = ActionTypeProto.valueOf(action.getType().name());
                    protoBuilder.setType(actionType);
                    log.debug("Установлен тип действия для датчика '{}': {}", sensorId, actionType);
                } catch (IllegalArgumentException e) {
                    log.error("Неподдерживаемый тип действия '{}' для датчика '{}'", action.getType(), sensorId, e);
                    continue;
                }
            } else {
                log.error("Тип действия не указан для датчика '{}'", sensorId);
                continue;
            }

            if (action.getValue() != null) {
                protoBuilder.setValue(action.getValue());
                log.debug("Установлено значение действия для датчика '{}': {}", sensorId, action.getValue());
            }

            DeviceActionProto proto = protoBuilder.build();

            Instant now = Instant.now();
            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(proto)
                    .setTimestamp(timestamp)
                    .build();

            try {
                routerClient.sendAction(request);
                log.info("Действие отправлено для сценария '{}', датчик '{}'", scenario.getName(), sensorId);
            } catch (Exception e) {
                log.error("Ошибка отправки действия для сценария '{}', датчик '{}'", scenario.getName(), sensorId, e);
            }
        }
    }
}