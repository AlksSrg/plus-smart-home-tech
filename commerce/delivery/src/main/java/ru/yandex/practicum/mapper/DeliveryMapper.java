package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.model.Address;
import ru.yandex.practicum.model.Delivery;

/**
 * Маппер для преобразования между сущностями и DTO доставки.
 * Использует MapStruct для автоматической генерации кода преобразования.
 */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface DeliveryMapper {

    /**
     * Преобразует DeliveryDto в сущность Delivery.
     *
     * @param deliveryDto DTO доставки
     * @return сущность Delivery
     */
    Delivery mapToDelivery(DeliveryDto deliveryDto);

    /**
     * Преобразует сущность Delivery в DeliveryDto.
     *
     * @param delivery сущность доставки
     * @return DTO доставки
     */
    @Mapping(source = "deliveryId", target = "deliveryId")
    @Mapping(source = "fromAddress", target = "fromAddress")
    @Mapping(source = "toAddress", target = "toAddress")
    @Mapping(source = "orderId", target = "orderId")
    @Mapping(source = "deliveryState", target = "deliveryState")
    DeliveryDto mapToDeliveryDto(Delivery delivery);

    /**
     * Преобразует AddressDto в сущность Address.
     *
     * @param addressDto DTO адреса
     * @return сущность Address
     */
    Address mapToAddress(AddressDto addressDto);

    /**
     * Преобразует сущность Address в AddressDto.
     *
     * @param address сущность адреса
     * @return DTO адреса
     */
    AddressDto mapToAddressDto(Address address);
}