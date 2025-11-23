package ru.yandex.practicum.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.HubEventType;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Сервис для обработки событий добавления сценариев к хабу.
 * Преобразует ScenarioAddedEvent в Avro формат и отправляет в Kafka.
 */
@Service
public class ScenarioAddedService extends HubEventService<ScenarioAddedEventAvro> {

    public ScenarioAddedService(KafkaProducerEvent kafkaProducerEvent,
                                @Value("${kafka.topics.hub-events:telemetry.hubs.v1}") String topicName) {
        super(kafkaProducerEvent, topicName);
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }

    @Override
    public ScenarioAddedEventAvro mapToAvro(HubEvent hubEvent) {
        ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) hubEvent;

        List<ScenarioConditionAvro> conditions = scenarioAddedEvent.getConditions().stream()
                .map(condition -> ScenarioConditionAvro.newBuilder()
                        .setSensorId(condition.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                        .setValue(mapConditionValue(condition.getValue()))
                        .build())
                .collect(Collectors.toList());

        List<DeviceActionAvro> actions = scenarioAddedEvent.getActions().stream()
                .map(action -> DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeAvro.valueOf(action.getType().name()))
                        .setValue((Integer) mapActionValue(action.getValue()))
                        .build())
                .collect(Collectors.toList());

        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    @Override
    protected HubEventAvro mapToAvroHubEvent(HubEvent hubEvent) {
        ScenarioAddedEventAvro payload = mapToAvro(hubEvent);
        return buildHubEventAvro(hubEvent, payload);
    }

    /**
     * Преобразует значение условия в совместимый с Avro тип.
     * По схеме: union{null, int, boolean} value = null
     */
    private Object mapConditionValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer || value instanceof Boolean) {
            return value;
        }
        // Попытка преобразования строки в int
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Неподдерживаемый тип значения условия: " + value.getClass());
            }
        }
        throw new IllegalArgumentException("Неподдерживаемый тип значения условия: " + value.getClass());
    }

    /**
     * Преобразует значение действия в совместимый с Avro тип.
     * По схеме: union{null, int} value = null
     */
    private Object mapActionValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return value;
        }
        // Попытка преобразования в int
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Неподдерживаемый тип значения действия: " + value.getClass());
            }
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalArgumentException("Неподдерживаемый тип значения действия: " + value.getClass());
    }
}