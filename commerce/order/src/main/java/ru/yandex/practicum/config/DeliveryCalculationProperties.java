package ru.yandex.practicum.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Конфигурационный класс для параметров расчета стоимости доставки.
 * Значения загружаются из application.yaml с префиксом "delivery.calculation".
 */
@Data
@Component
@ConfigurationProperties(prefix = "delivery.calculation")
public class DeliveryCalculationProperties {

    private BigDecimal basePrice = new BigDecimal("5.00");
    private BigDecimal minPrice = new BigDecimal("3.00");

    private Weight weight = new Weight();
    private Volume volume = new Volume();
    private Fragile fragile = new Fragile();

    @Data
    public static class Weight {
        private Double threshold = 10.0;
        private BigDecimal surchargePerKg = new BigDecimal("0.50");
    }

    @Data
    public static class Volume {
        private Double threshold = 1.0;
        private BigDecimal surchargePerCubicMeter = new BigDecimal("2.00");
    }

    @Data
    public static class Fragile {
        private BigDecimal surchargePercentage = new BigDecimal("0.20");
    }
}