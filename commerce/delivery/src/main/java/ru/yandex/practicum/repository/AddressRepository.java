package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Address;

import java.util.UUID;

/**
 * Репозиторий для работы с сущностями Address.
 * Предоставляет CRUD операции для адресов.
 */
@Repository
public interface AddressRepository extends JpaRepository<Address, UUID> {
}