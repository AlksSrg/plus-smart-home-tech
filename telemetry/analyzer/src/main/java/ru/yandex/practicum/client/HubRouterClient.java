package ru.yandex.practicum.client;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Slf4j
@Service
public class HubRouterClient {
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(
            @GrpcClient("hub-router")
            HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendRequest(DeviceActionRequest request) {
        log.info("Отправка запроса на выполнение действия: {}", request);
        try {
            hubRouterClient.handleDeviceAction(request);
            log.info("Запрос успешно отправлен в Hub Router");
        } catch (Exception e) {
            log.error("Ошибка при отправке запроса в Hub Router", e);
            throw new RuntimeException("Не удалось отправить запрос в Hub Router", e);
        }
    }
}