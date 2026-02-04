package ru.yandex.practicum.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Конфигурационный класс для параметров доставки.
 * Значения загружаются из application.yaml с префиксом "delivery".
 */
@Data
@Component
@ConfigurationProperties(prefix = "delivery")
public class DeliveryProperties {

    private Cost cost = new Cost();
    private Markup markup = new Markup();
    private Warehouse warehouse = new Warehouse();

    @Data
    public static class Cost {
        private Double base = 5.0;
        private Double weightPerUnit = 0.3;
        private Double volumePerUnit = 0.2;
    }

    @Data
    public static class Markup {
        private Double fragilePercentage = 0.2;
        private Double differentStreetPercentage = 0.2;

        private WarehouseMarkup warehouse = new WarehouseMarkup();

        @Data
        public static class WarehouseMarkup {
            private Double low = 1.0;
            private Double high = 2.0;
        }
    }

    @Data
    public static class Warehouse {
        private String addressIndicator = "ADDRESS_2";
    }
}