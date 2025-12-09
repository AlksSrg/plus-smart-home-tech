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
        log.info(
                "Отправка команды в HubRouter: hubId={}, сценарий='{}', действие={}",
                request.getHubId(),
                request.getScenarioName(),
                request.getAction() != null ? request.getAction() : "<нет действия>"
        );

        // Добавляем диагностическую информацию
        log.debug("Адрес сервиса HubRouter: {}", getServiceAddress());
        log.debug("Детали запроса: {}", request);

        try {
            // Согласно proto-схеме, метод возвращает google.protobuf.Empty
            Empty response = stub.handleDeviceAction(request);
            log.info("Команда успешно отправлена в HubRouter");

        } catch (StatusRuntimeException e) {
            log.error("Ошибка gRPC при вызове HubRouter: статус={}, описание={}",
                    e.getStatus().getCode(), e.getStatus().getDescription(), e);

            // Добавляем дополнительную диагностику
            log.error("Проверьте: 1) Запущен ли сервер HubRouter? 2) Корректно ли настроен адрес? 3) Реализован ли метод handleDeviceAction на сервере?");

            throw new RuntimeException("Ошибка отправки команды в HubRouter: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Неожиданная ошибка при вызове HubRouter: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Метод для диагностики - получение адреса сервиса.
     * (Временный метод для отладки)
     */
    private String getServiceAddress() {
        try {
            // Попробуем получить информацию о канале
            return stub.getChannel().authority();
        } catch (Exception e) {
            return "Не удалось получить адрес: " + e.getMessage();
        }
    }
}