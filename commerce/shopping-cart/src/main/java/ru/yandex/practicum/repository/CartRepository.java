package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.model.ShoppingCartStatus;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CartRepository extends JpaRepository<ShoppingCart, UUID> {
    Optional<ShoppingCart> findByUsername(String username);

    Optional<ShoppingCart> findByUsernameAndStatus(String username, ShoppingCartStatus status);

    boolean existsByUsernameAndStatus(String username, ShoppingCartStatus status);
}