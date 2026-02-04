package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.config.DeliveryCalculationProperties;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.exception.order.ServiceUnavailableException;
import ru.yandex.practicum.feign.ShoppingStoreClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.UUID;

/**
 * Сервис для расчета стоимостей заказа.
 * Выполняет расчет стоимости товаров и доставки.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PriceCalculatorService {

    private final ShoppingStoreClient shoppingStoreClient;
    private final DeliveryCalculationProperties deliveryProperties;

    /**
     * Рассчитывает общую стоимость товаров в заказе.
     *
     * @param items карта товаров (ID -> количество)
     * @return общая стоимость товаров
     * @throws ServiceUnavailableException если сервис магазина недоступен
     */
    public BigDecimal calculateItemsPrice(Map<UUID, Integer> items) {
        if (items == null || items.isEmpty()) {
            log.debug("Нет товаров для расчета стоимости");
            return BigDecimal.ZERO;
        }

        log.debug("Расчет стоимости товаров: количество позиций = {}", items.size());

        BigDecimal total = BigDecimal.ZERO;

        for (Map.Entry<UUID, Integer> entry : items.entrySet()) {
            UUID productId = entry.getKey();
            Integer quantity = entry.getValue();

            if (quantity <= 0) {
                log.warn("Некорректное количество товара {}: {}", productId, quantity);
                continue;
            }

            try {
                ProductDto product = shoppingStoreClient.getProductById(productId);

                BigDecimal price = BigDecimal.valueOf(product.getPrice())
                        .setScale(2, RoundingMode.HALF_UP);

                BigDecimal itemTotal = price.multiply(BigDecimal.valueOf(quantity))
                        .setScale(2, RoundingMode.HALF_UP);

                total = total.add(itemTotal);

                log.debug("Товар {}: цена={}, количество={}, сумма={}",
                        productId, price, quantity, itemTotal);

            } catch (Exception e) {
                log.error("Не удалось получить цену товара {}: {}", productId, e.getMessage());
                throw new ServiceUnavailableException("Не удалось рассчитать стоимость товара: " + productId, e);
            }
        }

        total = total.setScale(2, RoundingMode.HALF_UP);
        log.debug("Итоговая стоимость товаров: {}", total);
        return total;
    }

    /**
     * Рассчитывает стоимость доставки на основе параметров заказа.
     *
     * @param weight    вес заказа в килограммах
     * @param volume    объем заказа в кубических метрах
     * @param isFragile признак наличия хрупких товаров
     * @return стоимость доставки
     */
    public BigDecimal calculateDeliveryPrice(Double weight, Double volume, Boolean isFragile) {
        log.debug("Расчет стоимости доставки: вес={}, объем={}, хрупкий={}",
                weight, volume, isFragile);

        BigDecimal basePrice = deliveryProperties.getBasePrice();

        if (weight != null && weight > deliveryProperties.getWeight().getThreshold()) {
            BigDecimal weightSurcharge = BigDecimal.valueOf(weight - deliveryProperties.getWeight().getThreshold())
                    .multiply(deliveryProperties.getWeight().getSurchargePerKg())
                    .setScale(2, RoundingMode.HALF_UP);
            basePrice = basePrice.add(weightSurcharge);
            log.debug("Наценка за вес: {}", weightSurcharge);
        }

        if (volume != null && volume > deliveryProperties.getVolume().getThreshold()) {
            BigDecimal volumeSurcharge = BigDecimal.valueOf(volume - deliveryProperties.getVolume().getThreshold())
                    .multiply(deliveryProperties.getVolume().getSurchargePerCubicMeter())
                    .setScale(2, RoundingMode.HALF_UP);
            basePrice = basePrice.add(volumeSurcharge);
            log.debug("Наценка за объем: {}", volumeSurcharge);
        }

        if (Boolean.TRUE.equals(isFragile)) {
            BigDecimal fragileSurcharge = basePrice.multiply(deliveryProperties.getFragile().getSurchargePercentage())
                    .setScale(2, RoundingMode.HALF_UP);
            basePrice = basePrice.add(fragileSurcharge);
            log.debug("Наценка за хрупкость: {}", fragileSurcharge);
        }

        if (basePrice.compareTo(deliveryProperties.getMinPrice()) < 0) {
            basePrice = deliveryProperties.getMinPrice();
            log.debug("Установлена минимальная стоимость доставки: {}", deliveryProperties.getMinPrice());
        }

        basePrice = basePrice.setScale(2, RoundingMode.HALF_UP);

        log.debug("Итоговая стоимость доставки: {}", basePrice);
        return basePrice;
    }
}