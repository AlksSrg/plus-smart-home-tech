package ru.yandex.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerEvent {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(String topic, String key, Object value) {
        try {
            kafkaTemplate.send(topic, key, value)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Message sent successfully to topic: {}, key: {}", topic, key);
                        } else {
                            log.error("Failed to send message to topic: {}, key: {}", topic, key, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Error sending message to Kafka topic: {}", topic, e);
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }
}