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
        // 1. Извлекаем данные из события
        String hubId = extractHubId(event);
        if (hubId == null || hubId.isEmpty()) {
            log.warn("Invalid hubId in event: {}", event);
            return Optional.empty();
        }

        String sensorId = event.getId();
        if (sensorId == null || sensorId.isEmpty()) {
            log.warn("Invalid sensorId in event: {}", event);
            return Optional.empty();
        }

        long eventTimestamp = event.getTimestamp();
        Object eventPayload = event.getPayload();

        if (eventPayload == null) {
            log.warn("Event payload is null: {}", event);
            return Optional.empty();
        }

        log.debug("Processing event - hub: {}, sensor: {}, time: {}, payload type: {}",
                hubId, sensorId, eventTimestamp, eventPayload.getClass().getSimpleName());

        // 2. Получаем или создаем снапшот
        SensorsSnapshotAvro currentSnapshot = snapshots.computeIfAbsent(hubId, id -> {
            log.debug("Creating new snapshot for hub: {}", hubId);
            return createNewSnapshot(event, hubId, sensorId, eventTimestamp, eventPayload);
        });

        // 3. Проверяем нужно ли обновлять
        Map<String, SensorStateAvro> currentStates = currentSnapshot.getSensorsState();
        SensorStateAvro existingState = currentStates.get(sensorId);

        boolean shouldUpdate = false;

        if (existingState == null) {
            // Новый датчик - всегда обновляем
            log.debug("New sensor detected: {}", sensorId);
            shouldUpdate = true;
        } else {
            long existingTimestamp = existingState.getTimestamp();
            Object existingData = existingState.getData();

            // Сравниваем timestamp
            if (eventTimestamp > existingTimestamp) {
                log.debug("Newer timestamp: {} > {}", eventTimestamp, existingTimestamp);
                shouldUpdate = true;
            }
            // Если время одинаковое, сравниваем данные
            else if (eventTimestamp == existingTimestamp) {
                if (!isDataEqual(existingData, eventPayload)) {
                    log.debug("Same timestamp but different data");
                    shouldUpdate = true;
                } else {
                    log.debug("Same timestamp and data - no update needed");
                }
            } else {
                log.debug("Older timestamp: {} < {} - skipping", eventTimestamp, existingTimestamp);
            }
        }

        // 4. Обновляем если нужно
        if (shouldUpdate) {
            SensorsSnapshotAvro updatedSnapshot = updateSnapshot(
                    currentSnapshot, sensorId, eventTimestamp, eventPayload
            );
            snapshots.put(hubId, updatedSnapshot);

            log.info("Snapshot updated - hub: {}, sensors: {}, time: {}",
                    hubId, updatedSnapshot.getSensorsState().size(), eventTimestamp);
            return Optional.of(updatedSnapshot);
        }

        return Optional.empty();
    }

    /**
     * Извлекает hubId из события
     */
    private String extractHubId(SensorEventAvro event) {
        try {
            // Используем рефлексию для надежности
            for (String methodName : new String[]{"getHubId", "getHubid", "getHubID"}) {
                try {
                    java.lang.reflect.Method method = event.getClass().getMethod(methodName);
                    Object result = method.invoke(event);
                    if (result != null) {
                        return result.toString();
                    }
                } catch (NoSuchMethodException e) {
                    // Пробуем следующий вариант
                    continue;
                }
            }

            // Если стандартные методы не работают, пробуем toString
            String str = event.toString();
            if (str.contains("hubId=")) {
                return str.split("hubId=")[1].split(",")[0].trim();
            } else if (str.contains("hub_id=")) {
                return str.split("hub_id=")[1].split(",")[0].trim();
            }

        } catch (Exception e) {
            log.error("Error extracting hubId from event: {}", event, e);
        }

        return null;
    }

    /**
     * Безопасное сравнение данных
     */
    private boolean isDataEqual(Object data1, Object data2) {
        if (data1 == null && data2 == null) return true;
        if (data1 == null || data2 == null) return false;

        // Для Avro union используем equals и toString как fallback
        if (data1.equals(data2)) {
            return true;
        }

        // Иногда equals не работает для Avro union, используем toString
        return data1.toString().equals(data2.toString());
    }

    private SensorsSnapshotAvro createNewSnapshot(SensorEventAvro event, String hubId,
                                                  String sensorId, long timestamp, Object payload) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(sensorId, createSensorState(timestamp, payload));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(sensorStates)
                .build();
    }

    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot, String sensorId,
                                               long timestamp, Object payload) {
        // Создаем копию map (важно для иммутабельности)
        Map<String, SensorStateAvro> updatedStates = new HashMap<>(snapshot.getSensorsState());
        updatedStates.put(sensorId, createSensorState(timestamp, payload));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(snapshot.getHubId())
                .setTimestamp(timestamp) // Обновляем время снапшота
                .setSensorsState(updatedStates)
                .build();
    }

    private SensorStateAvro createSensorState(long timestamp, Object payload) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(timestamp)
                .setData(payload)
                .build();
    }
}