package ru.yandex.practicum.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Параметры для отправки сообщения в Kafka.
 */
@Builder
@Getter
@ToString
public class KafkaProducerParam {
    private final String topic;
    private final Integer partition;
    private final Long timestamp;
    private final String key;
    private final SpecificRecordBase value;

    /**
     * Проверяет валидность параметров.
     *
     * @return true если параметры валидны, иначе false
     */
    public boolean isValid() {
        return topic != null && !topic.trim().isEmpty()
                && key != null && !key.trim().isEmpty()
                && value != null;
    }
}