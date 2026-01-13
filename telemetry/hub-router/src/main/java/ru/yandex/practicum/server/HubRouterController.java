package ru.yandex.practicum.server;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

/**
 * gRPC сервер для обработки команд от Analyzer.
 * Принимает команды на выполнение действий устройств и логирует их.
 */
@Slf4j
@GrpcService
public class HubRouterController extends HubRouterControllerGrpc.HubRouterControllerImplBase {

    /**
     * Обрабатывает команду на выполнение действия устройства.
     *
     * @param deviceActionRequest запрос с командой для устройства
     * @param responseObserver поток для отправки ответа клиенту
     */
    @Override
    public void handleDeviceAction(DeviceActionRequest deviceActionRequest, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получена команда от Analyzer: hub={}, сценарий='{}', датчик={}, действие={}",
                    deviceActionRequest.getHubId(),
                    deviceActionRequest.getScenarioName(),
                    deviceActionRequest.getAction().getSensorId(),
                    deviceActionRequest.getAction().getType());

            log.debug("Полные детали запроса: {}", deviceActionRequest);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.info("Команда успешно обработана и подтверждение отправлено");

        } catch (Exception e) {
            log.error("Ошибка обработки команды: {}", e.getMessage(), e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }
}