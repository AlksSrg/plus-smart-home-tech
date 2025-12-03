package ru.yandex.practicum.kafka.telemetry.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationEventSnapshotImpl implements AggregationEventSnapshot {

    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        if (event == null || event.getHubId() == null || event.getId() == null) {
            log.warn("Получено некорректное событие: {}", event);
            return Optional.empty();
        }

        String hubId = event.getHubId();
        String sensorId = event.getId();
        long eventTimestamp = event.getTimestamp();

        SensorsSnapshotAvro currentSnapshot = snapshots.computeIfAbsent(hubId, id ->
                createNewSnapshot(event)
        );

        Map<String, SensorStateAvro> currentStates = currentSnapshot.getSensorsState();
        SensorStateAvro existingState = currentStates.get(sensorId);

        if (existingState != null) {
            long existingTimestamp = existingState.getTimestamp();

            if (eventTimestamp < existingTimestamp) {
                log.debug("Пропущено старое событие: датчик={}, новое время={}, старое время={}",
                        sensorId, eventTimestamp, existingTimestamp);
                return Optional.empty();
            }

            if (eventTimestamp == existingTimestamp &&
                    isDataEqual(existingState.getData(), event.getPayload())) {
                log.debug("Данные не изменились: датчик={}, время={}", sensorId, eventTimestamp);
                return Optional.empty();
            }
        }

        SensorsSnapshotAvro updatedSnapshot = updateSnapshot(currentSnapshot, event, sensorId);
        snapshots.put(hubId, updatedSnapshot);

        log.debug("Обновлен снапшот: хаб={}, датчик={}, время={}",
                hubId, sensorId, eventTimestamp);
        return Optional.of(updatedSnapshot);
    }

    /**
     * Безопасное сравнение данных с учетом Avro union типов
     */
    private boolean isDataEqual(Object data1, Object data2) {
        if (data1 == null && data2 == null) return true;
        if (data1 == null || data2 == null) return false;

        return data1.equals(data2) || data1.toString().equals(data2.toString());
    }

    private SensorsSnapshotAvro createNewSnapshot(SensorEventAvro event) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(event.getId(), createSensorState(event));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorStates)
                .build();
    }

    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot,
                                               SensorEventAvro event,
                                               String sensorId) {
        Map<String, SensorStateAvro> updatedStates = new HashMap<>(snapshot.getSensorsState());
        updatedStates.put(sensorId, createSensorState(event));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(snapshot.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(updatedStates)
                .build();
    }

    private SensorStateAvro createSensorState(SensorEventAvro event) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();
    }
}