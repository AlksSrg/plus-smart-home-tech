package ru.yandex.practicum.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.HubEventType;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;

/**
 * Сервис для обработки событий удаления сценариев из хаба.
 * Преобразует ScenarioRemovedEvent в Avro формат и отправляет в Kafka.
 */
@Service
public class ScenarioRemovedService extends HubEventService<ScenarioRemovedEventAvro> {

    public ScenarioRemovedService(KafkaProducerEvent kafkaProducerEvent,
                                  @Value("${kafka.topics.hub-events:telemetry.hubs.v1}") String topicName) {
        super(kafkaProducerEvent, topicName);
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }

    @Override
    public ScenarioRemovedEventAvro mapToAvro(HubEvent hubEvent) {
        ScenarioRemovedEvent scenarioRemovedEvent = (ScenarioRemovedEvent) hubEvent;

        return ScenarioRemovedEventAvro.newBuilder()
                .setName(scenarioRemovedEvent.getName())
                .build();
    }

    @Override
    protected HubEventAvro mapToAvroHubEvent(HubEvent hubEvent) {
        ScenarioRemovedEventAvro payload = mapToAvro(hubEvent);
        return buildHubEventAvro(hubEvent, payload);
    }
}