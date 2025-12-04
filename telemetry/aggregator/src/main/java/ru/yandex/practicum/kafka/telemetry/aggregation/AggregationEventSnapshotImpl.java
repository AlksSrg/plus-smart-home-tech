package ru.yandex.practicum.kafka.telemetry.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationEventSnapshotImpl implements AggregationEventSnapshot {

    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    /**
     * Обновляет состояние на основе нового события.
     *
     * @param event событие с датчика
     * @return Optional с обновленным снапшотом, если состояние изменилось
     */
    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        try {
            String hubId = event.getHubId();
            if (hubId == null || hubId.isEmpty()) {
                log.warn("Некорректный hubId в событии: {}", event);
                return Optional.empty();
            }

            String sensorId = event.getId();
            if (sensorId == null || sensorId.isEmpty()) {
                log.warn("Некорректный sensorId в событии: {}", event);
                return Optional.empty();
            }

            long eventTimestamp = event.getTimestamp();
            Object eventPayload = event.getPayload();

            // Получаем текущий снапшот или создаем новый
            SensorsSnapshotAvro currentSnapshot = snapshots.get(hubId);

            if (currentSnapshot == null) {
                SensorsSnapshotAvro newSnapshot = createNewSnapshot(hubId, sensorId, eventTimestamp, eventPayload);
                snapshots.put(hubId, newSnapshot);
                log.info("Создан первый снапшот для хаба: {}", hubId);
                return Optional.of(newSnapshot);
            }

            // Проверяем нужно ли обновлять существующий снапшот
            boolean shouldUpdate = shouldUpdateSnapshot(currentSnapshot, sensorId, eventTimestamp, eventPayload);

            if (shouldUpdate) {
                SensorsSnapshotAvro updatedSnapshot = updateSnapshot(currentSnapshot, sensorId, eventTimestamp, eventPayload);
                snapshots.put(hubId, updatedSnapshot);
                log.info("Снапшот обновлен для хаба: {}, датчиков: {}",
                        hubId, updatedSnapshot.getSensorsState().size());
                return Optional.of(updatedSnapshot);
            }

            return Optional.empty();

        } catch (Exception e) {
            log.error("Ошибка обработки события: {}", event, e);
            return Optional.empty();
        }
    }

    /**
     * Проверяет, нужно ли обновлять снапшот на основе нового события.
     */
    private boolean shouldUpdateSnapshot(SensorsSnapshotAvro snapshot, String sensorId,
                                         long newTimestamp, Object newPayload) {
        Map<String, SensorStateAvro> currentStates = snapshot.getSensorsState();
        SensorStateAvro existingState = currentStates.get(sensorId);

        if (existingState == null) {
            return true;
        }

        long existingTimestamp = existingState.getTimestamp();
        Object existingData = existingState.getData();

        if (newTimestamp > existingTimestamp) {
            return true;
        }

        if (newTimestamp == existingTimestamp && !isDataEqual(existingData, newPayload)) {
            return true;
        }

        return false;
    }

    /**
     * Сравнивает данные для Avro union типов.
     */
    private boolean isDataEqual(Object data1, Object data2) {
        if (data1 == null && data2 == null) return true;
        if (data1 == null || data2 == null) return false;

        if (data1.equals(data2)) {
            return true;
        }

        String str1 = data1.toString().trim();
        String str2 = data2.toString().trim();
        return str1.equals(str2);
    }

    /**
     * Создает новый снапшот для хаба.
     */
    private SensorsSnapshotAvro createNewSnapshot(String hubId, String sensorId,
                                                  long timestamp, Object payload) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(sensorId, createSensorState(timestamp, payload));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(sensorStates)
                .build();
    }

    /**
     * Обновляет существующий снапшот.
     */
    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot, String sensorId,
                                               long timestamp, Object payload) {
        Map<String, SensorStateAvro> updatedStates = new HashMap<>(snapshot.getSensorsState());
        updatedStates.put(sensorId, createSensorState(timestamp, payload));

        long latestTimestamp = Math.max(snapshot.getTimestamp(), timestamp);
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(snapshot.getHubId())
                .setTimestamp(latestTimestamp)
                .setSensorsState(updatedStates)
                .build();
    }

    /**
     * Создает состояние датчика.
     */
    private SensorStateAvro createSensorState(long timestamp, Object payload) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(timestamp)
                .setData(payload)
                .build();
    }

    /**
     * Возвращает копию всех снапшотов для отладки.
     */
    public Map<String, SensorsSnapshotAvro> getSnapshots() {
        return new HashMap<>(snapshots);
    }
}