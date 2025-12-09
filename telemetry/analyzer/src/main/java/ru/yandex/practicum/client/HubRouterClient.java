package ru.yandex.practicum.client;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.Sensor;

import java.time.Instant;

/**
 * Клиент для отправки действий на хаб через gRPC сервис HubRouter.
 * Конвертирует внутренние модели в protobuf сообщения и отправляет их.
 */
@Slf4j
@Service
public class HubRouterClient {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;

    /**
     * Создает клиент для взаимодействия с HubRouter через gRPC.
     *
     * @param hub gRPC stub для HubRouterController, создаваемый Spring Boot Starter
     */
    public HubRouterClient(@GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hub) {
        this.hubRouter = hub;
    }

    /**
     * Отправляет действие на хаб через HubRouter.
     *
     * @param scenarioAction действие сценария для отправки
     */
    public void sendAction(ScenarioAction scenarioAction) {
        log.info("Отправка действия для сценария: {}", scenarioAction);

        DeviceActionRequest actionRequest = mapToActionRequest(scenarioAction);
        if (actionRequest == null) {
            log.warn("Действие не было отправлено из-за некорректных данных");
            return;
        }

        log.info("Отправка действия для сценария: Имя = {}, HubId = {}",
                actionRequest.getScenarioName(),
                actionRequest.getHubId());

        try {
            hubRouter.handleDeviceAction(actionRequest);
            log.debug("Действие успешно отправлено через gRPC");
        } catch (Exception e) {
            log.error("Ошибка при отправке действия через gRPC", e);
        }
    }

    private DeviceActionRequest mapToActionRequest(ScenarioAction scenarioAction) {
        Scenario scenario = scenarioAction.getScenario();
        Sensor sensor = scenarioAction.getSensor();
        Action action = scenarioAction.getAction();

        // Проверка корректности данных
        if (scenario == null || sensor == null || action == null) {
            log.error("Некорректные данные в ScenarioAction: scenario={}, sensor={}, action={}",
                    scenario, sensor, action);
            return null;
        }

        if (action.getType() == ActionTypeAvro.SET_VALUE && action.getValue() == null) {
            log.warn("Действие SET_VALUE без значения для датчика {}", sensor.getId());
            return null;
        }

        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(sensor.getId())
                .setType(mapActionTypeToProto(action.getType()));

        if (action.getValue() != null) {
            actionBuilder.setValue(action.getValue());
        }

        return DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(actionBuilder.build())
                .setTimestamp(currentTimestamp())
                .build();
    }

    private ActionTypeProto mapActionTypeToProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    private Timestamp currentTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}