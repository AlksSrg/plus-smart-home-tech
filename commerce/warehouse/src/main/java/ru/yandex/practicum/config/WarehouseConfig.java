package ru.yandex.practicum.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.SecureRandom;

@Configuration
@Slf4j
public class WarehouseConfig {

    private static final String[] ADDRESSES = {"ADDRESS_1", "ADDRESS_2"};

    @Bean
    public String warehouseAddress() {
        SecureRandom secureRandom = new SecureRandom();
        String address = ADDRESSES[secureRandom.nextInt(ADDRESSES.length)];
        log.info("Warehouse initialized with address: {}", address);
        return address;
    }
}