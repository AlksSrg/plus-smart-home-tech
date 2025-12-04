package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Scenario;

import java.util.List;
import java.util.Optional;

/**
 * Репозиторий для работы с сущностью Scenario.
 * Предоставляет методы для поиска сценариев по хабу.
 */
public interface ScenarioRepository extends JpaRepository<Scenario, Long> {

    /**
     * Находит все сценарии для указанного хаба.
     *
     * @param hubId идентификатор хаба
     * @return список сценариев хаба
     */
    List<Scenario> findByHubId(String hubId);

    /**
     * Находит сценарий по идентификатору хаба и имени сценария.
     *
     * @param hubId идентификатор хаба
     * @param name  название сценария
     * @return Optional с сценарием, если найден
     */
    Optional<Scenario> findByHubIdAndName(String hubId, String name);
}