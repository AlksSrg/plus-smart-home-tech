package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.model.ScenarioConditionId;

import java.util.List;

/**
 * Репозиторий для работы с сущностью ScenarioCondition.
 * Предоставляет методы для работы со связями сценариев и условий.
 */
@Repository
public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {

    /**
     * Находит все связи условий для указанного сценария.
     *
     * @param scenario сценарий
     * @return список связей условий сценария
     */
    List<ScenarioCondition> findByScenario(Scenario scenario);

    /**
     * Удаляет все связи условий для указанного сценария.
     * Используется при удалении или обновлении сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}