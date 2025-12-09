package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.model.Sensor;

import java.util.Collection;
import java.util.Optional;

public interface SensorRepository extends JpaRepository<Sensor, String> {

    // Метод для проверки существования хотя бы одного датчика из списка
    boolean existsByIdAndHubId(String id, String hubId);

    // Метод для проверки существования всех датчиков из списка (используется в ScenarioAddedHandler)
    @Query("SELECT COUNT(s) = :expectedCount FROM Sensor s WHERE s.id IN :ids AND s.hubId = :hubId")
    boolean existsByIdInAndHubId(@Param("ids") Collection<String> ids,
                                 @Param("hubId") String hubId,
                                 @Param("expectedCount") long expectedCount);

    Optional<Sensor> findByIdAndHubId(String id, String hubId);
}