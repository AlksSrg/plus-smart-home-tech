package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Payment;

import java.util.UUID;

/**
 * Репозиторий для работы с сущностью Payment.
 * Предоставляет методы для доступа к данным платежей в базе данных.
 */
@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {
}