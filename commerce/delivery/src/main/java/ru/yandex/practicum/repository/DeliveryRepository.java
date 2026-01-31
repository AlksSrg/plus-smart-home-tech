package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Delivery;

import java.util.Optional;
import java.util.UUID;

/**
 * Репозиторий для работы с сущностями Delivery.
 * Предоставляет CRUD операции и кастомные запросы для доставок.
 */
@Repository
public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    /**
     * Находит доставку по идентификатору заказа.
     *
     * @param orderId идентификатор заказа
     * @return Optional с доставкой, если найдена
     */
    Optional<Delivery> findByOrderId(UUID orderId);

    /**
     * Проверяет существование доставки для указанного заказа.
     *
     * @param orderId идентификатор заказа
     * @return true если доставка существует
     */
    boolean existsByOrderId(UUID orderId);
}