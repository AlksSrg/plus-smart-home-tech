package ru.yandex.practicum.kafka.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Сериализатор для преобразования Avro объектов в байтовые массивы.
 * Поддерживает все классы, унаследованные от SpecificRecordBase.
 */
@Slf4j
public class GeneralAvroSerializer implements Serializer<SpecificRecordBase> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, SpecificRecordBase data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            writer.write(data, encoder);
            encoder.flush();

            log.trace("Avro сообщение успешно сериализовано. Топик: {}, Схема: {}",
                    topic, data.getSchema().getName());
            return out.toByteArray();
        } catch (IOException e) {
            log.error("Ошибка сериализации Avro сообщения. Топик: {}, Схема: {}, Данные: {}",
                    topic, data.getSchema(), data, e);
            throw new RuntimeException("Не удалось сериализовать Avro сообщение", e);
        }
    }

    @Override
    public void close() {
    }
}