package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Sensor;

import java.util.Collection;
import java.util.Optional;

/**
 * Репозиторий для работы с сущностью Sensor.
 * Предоставляет методы для работы с датчиками умного дома.
 */
@Repository
public interface SensorRepository extends JpaRepository<Sensor, String> {

    /**
     * Проверяет существование датчика с указанным идентификатором и хабом.
     *
     * @param id идентификатор датчика
     * @param hubId идентификатор хаба
     * @return true если датчик существует, иначе false
     */
    boolean existsByIdAndHubId(String id, String hubId);

    /**
     * Находит датчик по идентификатору и хабу.
     *
     * @param id идентификатор датчика
     * @param hubId идентификатор хаба
     * @return Optional с датчиком, если найден
     */
    Optional<Sensor> findByIdAndHubId(String id, String hubId);

    /**
     * Удаляет датчик по идентификатору и хабу.
     * Используется для безопасного удаления только принадлежащих хабу датчиков.
     *
     * @param id идентификатор датчика
     * @param hubId идентификатор хаба
     */
    void deleteByIdAndHubId(String id, String hubId);

    /**
     * Проверяет существование всех датчиков из списка для указанного хаба.
     *
     * @param ids список идентификаторов датчиков
     * @param hubId идентификатор хаба
     * @param expectedCount ожидаемое количество найденных датчиков
     * @return true если все датчики из списка существуют для хаба, иначе false
     */
    @Query("SELECT COUNT(s) = :expectedCount FROM Sensor s WHERE s.id IN :ids AND s.hubId = :hubId")
    boolean existsByIdInAndHubId(@Param("ids") Collection<String> ids,
                                 @Param("hubId") String hubId,
                                 @Param("expectedCount") long expectedCount);
}