package ru.yandex.practicum.controller.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.service.grpc.GrpcEventProcessingService;

/**
 * gRPC контроллер для приема событий от датчиков и хабов.
 * Реализует протокол определенный в proto-схемах.
 */
@Slf4j
@GrpcService
@RequiredArgsConstructor
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final GrpcEventProcessingService grpcEventProcessingService;

    /**
     * Обрабатывает события от датчиков.
     *
     * @param request          событие датчика в Protobuf формате
     * @param responseObserver поток для отправки ответа клиенту
     */
    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено gRPC событие датчика: тип={}, hub={}, sensor={}",
                    request.getPayloadCase(), request.getHubId(), request.getId());

            // Обрабатываем событие
            grpcEventProcessingService.processSensorEvent(request);

            // Отправляем успешный ответ (пустой)
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("gRPC событие датчика успешно обработано");

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события датчика: hub={}, sensor={}",
                    request.getHubId(), request.getId(), e);

            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Ошибка обработки события датчика: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * Обрабатывает события от хабов.
     *
     * @param request          событие хаба в Protobuf формате
     * @param responseObserver поток для отправки ответа клиенту
     */
    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено gRPC событие хаба: тип={}, hub={}",
                    request.getPayloadCase(), request.getHubId());

            // Обрабатываем событие
            grpcEventProcessingService.processHubEvent(request);

            // Отправляем успешный ответ (пустой)
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("gRPC событие хаба успешно обработано");

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события хаба: hub={}",
                    request.getHubId(), e);

            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Ошибка обработки события хаба: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}