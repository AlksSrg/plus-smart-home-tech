package ru.yandex.practicum.handler.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.client.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.*;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

/**
 * Упрощенный обработчик снапшотов для тестирования.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    public void handleSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.info("Обработка снапшота для хаба: {}", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.debug("Найдено {} сценариев для хаба: {}", scenarios.size(), hubId);

        for (Scenario scenario : scenarios) {
            try {
                // Простая проверка: если есть сценарий, отправляем тестовую команду
                log.info("Найден сценарий '{}' для хаба: {}", scenario.getName(), hubId);

                // Отправляем тестовую команду
                sendTestCommand(scenario, snapshot);
            } catch (Exception e) {
                log.error("Ошибка при обработке сценария для хаба: {}", hubId, e);
            }
        }
    }

    private void sendTestCommand(Scenario scenario, SensorsSnapshotAvro snapshot) {
        try {
            // Создаем DeviceActionProto
            DeviceActionProto action = DeviceActionProto.newBuilder()
                    .setSensorId("test-device-id")  // ID устройства
                    .setType(ActionTypeProto.SET_VALUE)  // Тип действия
                    .setValue(1)  // Значение
                    .build();

            // Создаем timestamp
            Instant now = Instant.now();
            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

            // Создаем DeviceActionRequest согласно прото-схеме
            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(snapshot.getHubId())
                    .setScenarioName(scenario.getName())  // Используем scenario_name, а не scenarioId
                    .setAction(action)  // Добавляем действие
                    .setTimestamp(timestamp)  // Добавляем timestamp
                    .build();

            hubRouterClient.sendRequest(request);
            log.info("Тестовая команда отправлена для сценария: {}", scenario.getName());
        } catch (Exception e) {
            log.error("Ошибка при отправке тестовой команды", e);
        }
    }
}