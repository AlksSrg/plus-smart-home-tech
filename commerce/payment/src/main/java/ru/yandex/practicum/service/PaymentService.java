package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.enums.PaymentState;
import ru.yandex.practicum.exception.payment.ImpossibleCalculateCostOrderException;
import ru.yandex.practicum.exception.payment.NoFoundPaymentException;
import ru.yandex.practicum.feign.OrderFeignClient;
import ru.yandex.practicum.feign.ShoppingStoreClient;
import ru.yandex.practicum.mapper.PaymentMapper;
import ru.yandex.practicum.model.Payment;
import ru.yandex.practicum.repository.PaymentRepository;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Сервис для работы с платежами.
 * Предоставляет бизнес-логику для расчета стоимости, создания и обработки платежей.
 */
@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentMapper paymentMapper;
    private final PaymentRepository paymentRepository;
    private final ShoppingStoreClient storeClient;
    private final OrderFeignClient orderClient;

    @Value("${payment.VAT:0.2}")
    private Double vat;

    /**
     * Создает платеж для заказа.
     *
     * @param orderDto информация о заказе
     * @return созданный платеж
     */
    @Transactional
    public PaymentDto makingPaymentForOrder(OrderDto orderDto) {
        log.info("Creating payment for order: {}", orderDto.getOrderId());

        // Проверяем, есть ли данные для расчета
        if (!orderDto.hasPaymentData()) {
            throw new ImpossibleCalculateCostOrderException(
                    String.format("Order with ID = %s does not have payment data", orderDto.getOrderId())
            );
        }

        Double productCost = calculateProductCostPayment(orderDto);
        Double totalCost = calculateTotalCostPayment(orderDto);

        Payment payment = Payment.builder()
                .orderId(orderDto.getOrderId())
                .totalPayment(totalCost)
                .deliveryTotal(orderDto.getDeliveryPrice())
                .feeTotal(productCost * vat)
                .state(PaymentState.PENDING)
                .build();

        Payment savedPayment = paymentRepository.save(payment);
        log.info("Payment created successfully: {} for order: {}", savedPayment.getPaymentId(), orderDto.getOrderId());

        return paymentMapper.mapToPaymentDto(savedPayment);
    }

    /**
     * Рассчитывает общую стоимость платежа.
     *
     * @param orderDto информация о заказе
     * @return общая стоимость
     * @throws ImpossibleCalculateCostOrderException если не удается рассчитать стоимость
     */
    public Double calculateTotalCostPayment(OrderDto orderDto) {
        log.debug("Calculating total cost for order: {}", orderDto.getOrderId());

        Double productPrice = orderDto.getProductPrice();
        Double deliveryPrice = orderDto.getDeliveryPrice();

        if (productPrice == null || deliveryPrice == null) {
            throw new ImpossibleCalculateCostOrderException(
                    String.format("The cost of an order with ID = %s cannot be calculated. " +
                                    "productPrice = %s, deliveryPrice = %s",
                            orderDto.getOrderId(), productPrice, deliveryPrice)
            );
        }

        Double total = productPrice + (productPrice * vat) + deliveryPrice;
        log.debug("Calculated total cost: {} for order: {}", total, orderDto.getOrderId());
        return total;
    }

    /**
     * Подтверждает успешный платеж.
     *
     * @param paymentId идентификатор платежа
     */
    @Transactional
    public void successfulPayment(UUID paymentId) {
        log.info("Processing successful payment: {}", paymentId);

        Payment payment = getPaymentById(paymentId);
        payment.setState(PaymentState.SUCCESS);
        paymentRepository.save(payment);

        // Уведомляем сервис заказов
        orderClient.paymentOrder(payment.getOrderId());

        log.info("Payment {} marked as SUCCESS for order: {}", paymentId, payment.getOrderId());
    }

    /**
     * Рассчитывает стоимость товаров в заказе.
     *
     * @param orderDto информация о заказе
     * @return стоимость товаров
     */
    public Double calculateProductCostPayment(OrderDto orderDto) {
        log.debug("Calculating product cost for order: {}", orderDto.getOrderId());

        Map<UUID, Integer> products = orderDto.getProducts();

        if (products == null || products.isEmpty()) {
            log.warn("No products found in order: {}", orderDto.getOrderId());
            return 0.0;
        }

        // Получаем цены всех товаров
        Map<UUID, Float> priceMap = products.keySet().stream()
                .map(storeClient::getProductById)
                .collect(Collectors.toMap(ProductDto::getProductId, ProductDto::getPrice));

        // Рассчитываем общую стоимость
        Double totalCost = products.entrySet().stream()
                .mapToDouble(entry -> {
                    UUID productId = entry.getKey();
                    Integer quantity = entry.getValue();
                    Float price = priceMap.get(productId);

                    if (price == null) {
                        log.warn("Price not found for product: {}", productId);
                        return 0.0;
                    }

                    return quantity * price;
                })
                .sum();

        log.debug("Calculated product cost: {} for order: {}", totalCost, orderDto.getOrderId());
        return totalCost;
    }

    /**
     * Обрабатывает неудачный платеж.
     *
     * @param paymentId идентификатор платежа
     */
    @Transactional
    public void failedPayment(UUID paymentId) {
        log.info("Processing failed payment: {}", paymentId);

        Payment payment = getPaymentById(paymentId);
        payment.setState(PaymentState.FAILED);
        paymentRepository.save(payment);

        // Уведомляем сервис заказов
        orderClient.paymentOrderFailed(payment.getOrderId());

        log.info("Payment {} marked as FAILED for order: {}", paymentId, payment.getOrderId());
    }

    /**
     * Получает платеж по идентификатору.
     *
     * @param paymentId идентификатор платежа
     * @return найденный платеж
     * @throws NoFoundPaymentException если платеж не найден
     */
    private Payment getPaymentById(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoFoundPaymentException(
                        String.format("Payment with ID = %s not found.", paymentId)
                ));
    }
}