package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Sensor;

import java.util.Collection;
import java.util.Optional;

/**
 * Репозиторий для работы с сущностью Sensor.
 * Предоставляет методы для проверки существования датчиков.
 */
public interface SensorRepository extends JpaRepository<Sensor, String> {

    /**
     * Проверяет существование всех указанных датчиков для хаба.
     *
     * @param ids   коллекция идентификаторов датчиков
     * @param hubId идентификатор хаба
     * @return true если все датчики существуют для указанного хаба
     */
    boolean existsByIdInAndHubId(Collection<String> ids, String hubId);

    /**
     * Находит датчик по идентификатору и хабу.
     *
     * @param id    идентификатор датчика
     * @param hubId идентификатор хаба
     * @return Optional с датчиком, если найден
     */
    Optional<Sensor> findByIdAndHubId(String id, String hubId);
}