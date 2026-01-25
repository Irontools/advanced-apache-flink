package dev.irontools.flink.workshop.join;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * Process Table Function implementing buffered join between Products and Orders.
 *
 * This PTF demonstrates:
 * - Multi-table input processing
 * - MapView state for buffering
 * - Timer-based timeout handling
 * - Symmetric join logic
 *
 * The function matches products with orders within a configurable timeout window.
 * Unmatched records are emitted when the timeout expires.
 */
@FunctionHint(
    output = @DataTypeHint("ROW<"
        + "join_type STRING, "
        + "product_id STRING, "
        + "product_name STRING, "
        + "price DECIMAL(10, 2), "
        + "order_id STRING, "
        + "customer_name STRING, "
        + "amount DECIMAL(10, 2)"
        + ">")
)
public class JoinSolutionPTF extends ProcessTableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(JoinSolutionPTF.class);

    private static final String PRODUCT_TIMEOUT_TIMER = "PRODUCT_TIMEOUT";
    private static final String ORDER_TIMEOUT_TIMER = "ORDER_TIMEOUT";

    private static final long TIMEOUT_MS = 10_000L;

    /**
     * State container for timer timestamps.
     * MapView state is declared as eval() parameters with @StateHint.
     */
    public static class TimerState {
        public Long productTimerTimestamp;
        public Long orderTimerTimestamp;
    }

    /**
     * Product data holder for buffering.
     */
    public static class ProductData {
        public String productId;
        public String productName;
        @DataTypeHint("DECIMAL(10, 2)")
        public BigDecimal price;
        public Long updateTime;

        public ProductData() {}

        public ProductData(String productId, String productName, BigDecimal price, Long updateTime) {
            this.productId = productId;
            this.productName = productName;
            this.price = price;
            this.updateTime = updateTime;
        }
    }

    /**
     * Order data holder for buffering.
     */
    public static class OrderData {
        public String orderId;
        public String customerName;
        @DataTypeHint("DECIMAL(10, 2)")
        public BigDecimal amount;
        public Long orderTime;

        public OrderData() {}

        public OrderData(String orderId, String customerName, BigDecimal amount, Long orderTime) {
            this.orderId = orderId;
            this.customerName = customerName;
            this.amount = amount;
            this.orderTime = orderTime;
        }
    }

    /**
     * Main evaluation method processing rows from either Products or Orders table.
     * Only one parameter is non-null per invocation.
     *
     * @param ctx PTF context providing time and timer services
     * @param timerState Timer timestamp tracking
     * @param productBuffer MapView state for buffering products
     * @param orderBuffer MapView state for buffering orders
     * @param product Row from Products table (null if order arrived)
     * @param order Row from Orders table (null if product arrived)
     */
    public void eval(
        Context ctx,
        @StateHint(ttl = "1 hour") TimerState timerState,
        @StateHint
        @DataTypeHint("MAP<STRING, ROW<productId STRING, productName STRING, price DECIMAL(10, 2), updateTime BIGINT>>")
        MapView<String, ProductData> productBuffer,
        @StateHint
        @DataTypeHint("MAP<STRING, ROW<orderId STRING, customerName STRING, amount DECIMAL(10, 2), orderTime BIGINT>>")
        MapView<String, OrderData> orderBuffer,
        @ArgumentHint(SET_SEMANTIC_TABLE) Row product,
        @ArgumentHint(SET_SEMANTIC_TABLE) Row order
    ) throws Exception {

        TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
        Instant eventTime = timeCtx.time();

        if (product != null) {
            handleProduct(product, eventTime, timeCtx, timerState, productBuffer, orderBuffer);
        } else if (order != null) {
            handleOrder(order, eventTime, timeCtx, timerState, productBuffer, orderBuffer);
        }
    }

    /**
     * Handles incoming product records.
     * Checks for matching orders in buffer, emits match or buffers product.
     */
    private void handleProduct(
        Row product,
        Instant eventTime,
        TimeContext<Instant> timeCtx,
        TimerState timerState,
        MapView<String, ProductData> productBuffer,
        MapView<String, OrderData> orderBuffer
    ) throws Exception {

        String productId = product.getFieldAs("product_id");
        String productName = product.getFieldAs("product_name");
        BigDecimal price = product.getFieldAs("price");
        Long updateTime = product.getFieldAs("update_time");

        LOG.info("[{}] Product received: {} at price {}", productId, productId, price);

        // Check if any order is waiting in the buffer
        Iterator<Map.Entry<String, OrderData>> orderIterator = orderBuffer.iterator();

        if (orderIterator.hasNext()) {
            // Match found: emit result and clean up
            Map.Entry<String, OrderData> orderEntry = orderIterator.next();
            OrderData matchedOrder = orderEntry.getValue();

            LOG.info("[{}] Match found! Product {} matched with order {}",
                productId, productId, matchedOrder.orderId);

            emitMatched(productId, productName, price,
                matchedOrder.orderId, matchedOrder.customerName, matchedOrder.amount);

            // Remove matched order from buffer
            orderBuffer.remove(orderEntry.getKey());

            // Clear order timer state if buffer is now empty
            boolean hasMoreOrders = orderBuffer.iterator().hasNext();
            if (!hasMoreOrders) {
                timerState.orderTimerTimestamp = null;
            }
        } else {
            // No match: buffer product and register timeout timer
            LOG.info("[{}] No matching order found, buffering product {}", productId, productId);

            ProductData productData = new ProductData(productId, productName, price, updateTime);
            productBuffer.put(productId, productData);

            // Register timer only if not already set
            if (timerState.productTimerTimestamp == null) {
                Instant timeoutTime = eventTime.plusMillis(TIMEOUT_MS);
                timeCtx.registerOnTime(PRODUCT_TIMEOUT_TIMER, timeoutTime);
                timerState.productTimerTimestamp = timeoutTime.toEpochMilli();

                LOG.info("[{}] Registered product timeout timer at {}", productId, timeoutTime);
            }
        }
    }

    /**
     * Handles incoming order records.
     * Checks for matching products in buffer, emits match or buffers order.
     */
    private void handleOrder(
        Row order,
        Instant eventTime,
        TimeContext<Instant> timeCtx,
        TimerState timerState,
        MapView<String, ProductData> productBuffer,
        MapView<String, OrderData> orderBuffer
    ) throws Exception {

        String orderId = order.getFieldAs("order_id");
        String customerName = order.getFieldAs("customer_name");
        String productId = order.getFieldAs("product_id");
        BigDecimal amount = order.getFieldAs("amount");
        Long orderTime = order.getFieldAs("timestamp");

        LOG.info("[{}] Order received: {} with amount {}", productId, orderId, amount);

        // Check if matching product exists in buffer
        ProductData matchedProduct = productBuffer.get(productId);

        if (matchedProduct != null) {
            // Match found: emit result and clean up
            LOG.info("[{}] Match found! Order {} matched with product {}",
                productId, orderId, matchedProduct.productId);

            emitMatched(productId, matchedProduct.productName, matchedProduct.price,
                orderId, customerName, amount);

            // Remove matched product from buffer
            productBuffer.remove(productId);

            // Clear product timer state if buffer is now empty
            boolean hasMoreProducts = productBuffer.iterator().hasNext();
            if (!hasMoreProducts) {
                timerState.productTimerTimestamp = null;
            }
        } else {
            // No match: buffer order and register timeout timer
            LOG.info("[{}] No matching product found, buffering order {}", productId, orderId);

            OrderData orderData = new OrderData(orderId, customerName, amount, orderTime);
            orderBuffer.put(orderId, orderData);

            // Register timer only if not already set
            if (timerState.orderTimerTimestamp == null) {
                Instant timeoutTime = eventTime.plusMillis(TIMEOUT_MS);
                timeCtx.registerOnTime(ORDER_TIMEOUT_TIMER, timeoutTime);
                timerState.orderTimerTimestamp = timeoutTime.toEpochMilli();

                LOG.info("[{}] Registered order timeout timer at {}", productId, timeoutTime);
            }
        }
    }

    /**
     * Timer callback handling product and order timeouts.
     * Emits unmatched records and clears buffers.
     */
    public void onTimer(
        OnTimerContext ctx,
        TimerState timerState,
        MapView<String, ProductData> productBuffer,
        MapView<String, OrderData> orderBuffer
    ) throws Exception {
        String timerName = ctx.currentTimer();

        LOG.info("Timer fired: {}", timerName);

        if (PRODUCT_TIMEOUT_TIMER.equals(timerName)) {
            Long productTimer = timerState.productTimerTimestamp;
            if (productTimer != null) {
                LOG.info("Product timeout triggered");

                // Emit all buffered products as unmatched
                Iterator<Map.Entry<String, ProductData>> iterator = productBuffer.iterator();
                while (iterator.hasNext()) {
                    ProductData product = iterator.next().getValue();
                    LOG.info("Emitting unmatched product: {}", product.productId);
                    emitUnmatchedProduct(product.productId, product.productName, product.price);
                }

                // Clear product buffer and timer state
                productBuffer.clear();
                timerState.productTimerTimestamp = null;
            }
        } else if (ORDER_TIMEOUT_TIMER.equals(timerName)) {
            Long orderTimer = timerState.orderTimerTimestamp;
            if (orderTimer != null) {
                LOG.info("Order timeout triggered");

                // Emit all buffered orders as unmatched
                Iterator<Map.Entry<String, OrderData>> iterator = orderBuffer.iterator();
                while (iterator.hasNext()) {
                    OrderData order = iterator.next().getValue();
                    LOG.info("Emitting unmatched order: {}", order.orderId);
                    emitUnmatchedOrder(order.orderId, order.customerName, order.amount);
                }

                // Clear order buffer and timer state
                orderBuffer.clear();
                timerState.orderTimerTimestamp = null;
            }
        }
    }

    private void emitMatched(
        String productId,
        String productName,
        BigDecimal price,
        String orderId,
        String customerName,
        BigDecimal amount
    ) {
        collect(Row.of("MATCHED", productId, productName, price, orderId, customerName, amount));
    }

    private void emitUnmatchedProduct(String productId, String productName, BigDecimal price) {
        collect(Row.of("UNMATCHED_PRODUCT", productId, productName, price, null, null, null));
    }

    private void emitUnmatchedOrder(String orderId, String customerName, BigDecimal amount) {
        collect(Row.of("UNMATCHED_ORDER", null, null, null, orderId, customerName, amount));
    }
}
