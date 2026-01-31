package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.enums.OrderStatus;
import ru.yandex.practicum.model.Order;

import java.util.List;
import java.util.UUID;

/**
 * Репозиторий для работы с сущностями заказов.
 * Предоставляет методы доступа к данным заказов в базе данных.
 */
@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    /**
     * Находит все заказы пользователя с пагинацией.
     *
     * @param username имя пользователя
     * @param pageable параметры пагинации
     * @return страница с заказами пользователя
     */
    Page<Order> findByUsername(String username, Pageable pageable);

    /**
     * Находит все заказы пользователя, отсортированные по дате создания (новые первые).
     *
     * @param username имя пользователя
     * @return список заказов пользователя
     */
    List<Order> findByUsernameOrderByCreatedAtDesc(String username);

    /**
     * Находит заказы пользователя по указанному статусу.
     *
     * @param username имя пользователя
     * @param status   статус заказа
     * @return список заказов с указанным статусом
     */
    List<Order> findByUsernameAndStatus(String username, OrderStatus status);

    /**
     * Проверяет существование заказа для указанной корзины.
     *
     * @param cartId идентификатор корзины
     * @return true если заказ существует, иначе false
     */
    boolean existsByCartId(UUID cartId);

    /**
     * Находит заказы по статусу.
     *
     * @param status статус заказа
     * @return список заказов с указанным статусом
     */
    List<Order> findByStatus(OrderStatus status);
}