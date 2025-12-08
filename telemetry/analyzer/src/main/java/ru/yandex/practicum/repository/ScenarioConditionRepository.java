package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;

import java.util.List;

/**
 * Репозиторий для работы с сущностью ScenarioCondition.
 * Предоставляет методы для работы со связями сценариев и условий.
 */
public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, Long> {

    /**
     * Находит все связи условий для указанного сценария.
     *
     * @param scenario сценарий
     * @return список связей условий сценария
     */
    List<ScenarioCondition> findByScenario(Scenario scenario);

    /**
     * Удаляет все связи условий для указанного сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}