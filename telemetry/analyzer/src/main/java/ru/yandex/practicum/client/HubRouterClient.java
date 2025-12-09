package ru.yandex.practicum.client;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionResponse;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

/**
 * gRPC-клиент для отправки команд в HubRouter.
 */
@Slf4j
@Service
public class HubRouterClient {

    /**
     * gRPC-stub для вызова HubRouterController.
     */
    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub stub;

    /**
     * Отправляет команду устройства в HubRouter.
     *
     * @param request запрос DeviceActionRequest
     */
    public void sendAction(DeviceActionRequest request) {
        log.info(
                "Отправка команды в HubRouter: hubId={}, сценарий='{}', действие={}",
                request.getHubId(),
                request.getScenarioName(),
                request.hasAction() ? request.getAction() : "<нет действия>"
        );

        try {
            DeviceActionResponse response = stub.handleDeviceAction(request);

            if (response.getSuccess()) {
                log.info("Команда успешно выполнена: {}", response.getMessage());
            } else {
                log.warn("HubRouter вернул ошибку: {}", response.getMessage());
            }

        } catch (Exception e) {
            log.error("Ошибка при вызове HubRouter: {}", e.getMessage(), e);
            throw e;
        }
    }
}
