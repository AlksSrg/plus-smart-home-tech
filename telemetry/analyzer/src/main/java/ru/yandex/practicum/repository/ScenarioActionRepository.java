package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;

import java.util.List;

/**
 * Репозиторий для работы с сущностью ScenarioAction.
 * Предоставляет методы для работы со связями сценариев и действий.
 */
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, Long> {

    /**
     * Находит все связи действий для указанного сценария.
     *
     * @param scenario сценарий
     * @return список связей действий сценария
     */
    List<ScenarioAction> findByScenario(Scenario scenario);

    /**
     * Удаляет все связи действий для указанного сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}