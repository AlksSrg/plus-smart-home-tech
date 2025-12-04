package ru.yandex.practicum.service.grpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.service.hub.HubEventProcessingService;
import ru.yandex.practicum.service.sensor.SensorEventProcessingService;
import ru.yandex.practicum.transformer.GrpcToDomainTransformer;


/**
 * Сервис для обработки gRPC событий.
 * Преобразует Protobuf сообщения в доменные модели и делегирует обработку.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GrpcEventProcessingService {

    private final GrpcToDomainTransformer transformer;
    private final SensorEventProcessingService sensorEventProcessingService;
    private final HubEventProcessingService hubEventProcessingService;

    /**
     * Обрабатывает событие датчика из gRPC.
     *
     * @param sensorEventProto Protobuf событие датчика
     */
    public void processSensorEvent(SensorEventProto sensorEventProto) {
        try {
            log.debug("Начинаем обработку gRPC события датчика: {}", sensorEventProto.getId());

            // Преобразуем Protobuf в доменную модель
            SensorEvent sensorEvent = transformer.toSensorEvent(sensorEventProto);

            // Делегируем обработку существующему сервису
            sensorEventProcessingService.process(sensorEvent);

            log.debug("gRPC событие датчика успешно преобразовано и обработано: {}", sensorEvent.getId());

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события датчика: {}", sensorEventProto.getId(), e);
            throw new RuntimeException("Не удалось обработать событие датчика", e);
        }
    }

    /**
     * Обрабатывает событие хаба из gRPC.
     *
     * @param hubEventProto Protobuf событие хаба
     */
    public void processHubEvent(HubEventProto hubEventProto) {
        try {
            log.debug("Начинаем обработку gRPC события хаба: {}", hubEventProto.getHubId());

            // Преобразуем Protobuf в доменную модель
            HubEvent hubEvent = transformer.toHubEvent(hubEventProto);

            // Делегируем обработку существующему сервису
            hubEventProcessingService.process(hubEvent);

            log.debug("gRPC событие хаба успешно преобразовано и обработано: {}", hubEvent.getHubId());

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события хаба: {}", hubEventProto.getHubId(), e);
            throw new RuntimeException("Не удалось обработать событие хаба", e);
        }
    }
}