package ru.yandex.practicum.client;

import com.google.protobuf.Empty;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
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
        try {
            stub.handleDeviceAction(request);
            log.info("Команда успешно отправлена в HubRouter для хаба {}", request.getHubId());
        } catch (StatusRuntimeException e) {
            log.error("Ошибка gRPC при вызове HubRouter: {}", e.getStatus().getDescription(), e);
            throw new RuntimeException("Ошибка отправки команды в HubRouter: " + e.getMessage(), e);
        }
    }
}