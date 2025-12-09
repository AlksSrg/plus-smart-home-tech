package ru.yandex.practicum.client;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionResponse;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import com.google.protobuf.Timestamp;

import java.time.Instant;

@Slf4j
@Service
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;

    /**
     * Отправляет действие в HubRouter.
     *
     * @param scenarioAction действие сценария
     */
    public void sendAction(ScenarioAction scenarioAction) {
        try {
            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(scenarioAction.getSensor().getId())
                    .setType(ActionTypeProto.valueOf(scenarioAction.getAction().getType().name()));

            if (scenarioAction.getAction().getValue() != null) {
                actionBuilder.setValue(scenarioAction.getAction().getValue());
            }

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenarioAction.getScenario().getHubId())
                    .setScenarioName(scenarioAction.getScenario().getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(timestamp())
                    .build();

            log.info("Отправка gRPC команды: {}", request);

            DeviceActionResponse response = hubRouter.handleDeviceAction(request);
            log.info("Ответ HubRouter: success={}, message={}",
                    response.getSuccess(), response.getMessage());

        } catch (Exception e) {
            log.error("Ошибка при отправке gRPC команды в HubRouter: {}", e.getMessage(), e);
        }
    }

    private Timestamp timestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }
}
