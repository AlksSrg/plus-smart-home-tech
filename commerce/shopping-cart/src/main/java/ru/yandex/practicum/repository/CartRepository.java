package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.model.ShoppingCartStatus;

import java.util.Optional;
import java.util.UUID;

/**
 * Репозиторий для работы с сущностью ShoppingCart.
 * Предоставляет методы для доступа к данным корзин в базе данных.
 */
@Repository
public interface CartRepository extends JpaRepository<ShoppingCart, UUID> {

    /**
     * Находит корзину по имени пользователя.
     *
     * @param username имя пользователя
     * @return Optional с корзиной, если найдена
     */
    Optional<ShoppingCart> findByUsername(String username);

    /**
     * Находит корзину по имени пользователя и статусу.
     *
     * @param username имя пользователя
     * @param status   статус корзины
     * @return Optional с корзиной, если найдена
     */
    Optional<ShoppingCart> findByUsernameAndStatus(String username, ShoppingCartStatus status);

    /**
     * Проверяет существование корзины по имени пользователя и статусу.
     *
     * @param username имя пользователя
     * @param status   статус корзины
     * @return true если корзина существует, иначе false
     */
    boolean existsByUsernameAndStatus(String username, ShoppingCartStatus status);
}