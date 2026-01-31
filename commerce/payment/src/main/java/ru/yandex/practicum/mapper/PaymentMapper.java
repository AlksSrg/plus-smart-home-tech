package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.model.Payment;


/**
 * Маппер для преобразования между сущностью Payment и DTO.
 * Использует MapStruct для автоматической генерации кода преобразования.
 */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface PaymentMapper {

    /**
     * Преобразует сущность Payment в DTO PaymentDto.
     *
     * @param payment сущность платежа
     * @return DTO с информацией о платеже
     */
    PaymentDto mapToPaymentDto(Payment payment);

    /**
     * Преобразует DTO PaymentDto в сущность Payment.
     *
     * @param paymentDto DTO с информацией о платеже
     * @return сущность платежа
     */
    Payment mapToPayment(PaymentDto paymentDto);
}