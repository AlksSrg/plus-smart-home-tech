package ru.yandex.practicum.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.enums.OrderStatus;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для представления заказа в системе.
 * Используется как для полного представления заказа, так и для расчета стоимости доставки.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {

    /**
     * Идентификатор заказа.
     */
    private UUID orderId;

    /**
     * Идентификатор корзины.
     */
    private UUID shoppingCartId;

    /**
     * Отображение идентификатора товара на отобранное количество.
     */
    private Map<UUID, Integer> products;

    /**
     * Идентификатор оплаты.
     */
    private UUID paymentId;

    /**
     * Идентификатор доставки.
     */
    private UUID deliveryId;

    /**
     * Статус заказа.
     */
    private OrderStatus state;

    /**
     * Общий вес товаров в заказе (в килограммах).
     * Используется для расчета стоимости доставки.
     */
    private Double deliveryWeight;

    /**
     * Общий объём товаров в заказе (в кубических метрах).
     * Используется для расчета стоимости доставки.
     */
    private Double deliveryVolume;

    /**
     * Признак хрупкости товаров в заказе.
     * true - товары хрупкие, влияет на стоимость доставки.
     */
    private Boolean fragile;

    /**
     * Общая стоимость заказа.
     */
    private Double totalPrice;

    /**
     * Стоимость доставки заказа.
     */
    private Double deliveryPrice;

    /**
     * Стоимость товаров в заказе.
     */
    private Double productPrice;

    /**
     * Проверяет, есть ли необходимые данные для расчета платежа.
     *
     * @return true если есть данные для расчета платежа, иначе false
     */
    public boolean hasPaymentData() {
        return productPrice != null && deliveryPrice != null && totalPrice != null;
    }

    /**
     * Проверяет, есть ли необходимые данные для расчета стоимости доставки.
     *
     * @return true если есть данные для расчета доставки, иначе false
     */
    public boolean hasDeliveryCalculationData() {
        return deliveryWeight != null && deliveryVolume != null && fragile != null;
    }
}