package dev.irontools.flink.workshop.join;

import dev.irontools.flink.Order;
import dev.irontools.flink.OrderGenerator;
import dev.irontools.flink.Product;
import dev.irontools.flink.ProductGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

/**
 * Solution: Symmetric buffered join between orders and products.
 *
 * This implementation demonstrates:
 * 1. Buffering both streams while waiting for matches
 * 2. Timer-based timeout handling
 * 3. Proper state management and cleanup
 * 4. Symmetric processing logic for both sides
 */
public class JoinSolution {

    private static final Logger LOG = LoggerFactory.getLogger(JoinSolution.class);
    private static final long TIMEOUT_MS = 10_000L;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Generate product data: 5 products with updates
        DataStream<Product> products = env.fromCollection(
            ProductGenerator.generateProductsWithUpdates(5, 2, 2000).iterator(),
            ProductGenerator.ProductWithMetadata.class
        )
        .map(ProductGenerator.ProductWithMetadata::getProduct)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Product>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((product, timestamp) -> product.getUpdateTime())
        );

        // Generate order data: 30 orders with random product assignments
        DataStream<Order> orders = env.fromCollection(
            OrderGenerator.generateOrdersWithDelay(30, 10, 3000, false).iterator(),
            Order.class
        )
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
        );

        // Connect the two streams keyed by productId and apply buffered join
        products
            .keyBy(Product::getProductId)
            .connect(orders.keyBy(Order::getProductId))
            .process(new BufferedJoinFunction(TIMEOUT_MS))
            .print();

        env.execute("Buffered Join Solution");
    }

    /**
     * Symmetric buffered join implementation using KeyedCoProcessFunction.
     *
     * State management:
     * - Products and orders are buffered separately when no match exists
     * - Timers track when buffered records should timeout
     * - State is cleaned up after matches or timeouts
     *
     * Note: This simplified implementation matches one product with one order.
     * For 1-to-many joins, modify to emit all combinations before clearing buffers.
     */
    public static class BufferedJoinFunction
            extends KeyedCoProcessFunction<String, Product, Order, JoinExercise.JoinResult> {

        private final long timeoutMs;

        // Buffered products waiting for matching orders (key: productId for deduplication)
        private transient MapState<String, Product> productBuffer;

        // Buffered orders waiting for matching products (key: orderId for uniqueness)
        private transient MapState<String, Order> orderBuffer;

        // Track when product timeout timer was registered
        private transient ValueState<Long> productTimerState;

        // Track when order timeout timer was registered
        private transient ValueState<Long> orderTimerState;

        public BufferedJoinFunction(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            MapStateDescriptor<String, Product> productBufferDescriptor =
                new MapStateDescriptor<>(
                    "product-buffer",
                    TypeInformation.of(String.class),
                    TypeInformation.of(Product.class)
                );
            productBuffer = getRuntimeContext().getMapState(productBufferDescriptor);

            MapStateDescriptor<String, Order> orderBufferDescriptor =
                new MapStateDescriptor<>(
                    "order-buffer",
                    TypeInformation.of(String.class),
                    TypeInformation.of(Order.class)
                );
            orderBuffer = getRuntimeContext().getMapState(orderBufferDescriptor);

            ValueStateDescriptor<Long> productTimerDescriptor =
                new ValueStateDescriptor<>(
                    "product-timer",
                    TypeInformation.of(Long.class)
                );
            productTimerState = getRuntimeContext().getState(productTimerDescriptor);

            ValueStateDescriptor<Long> orderTimerDescriptor =
                new ValueStateDescriptor<>(
                    "order-timer",
                    TypeInformation.of(Long.class)
                );
            orderTimerState = getRuntimeContext().getState(orderTimerDescriptor);
        }

        @Override
        public void processElement1(
                Product product,
                Context ctx,
                Collector<JoinExercise.JoinResult> out) throws Exception {

            LOG.info("[{}] Product received: {} at price {}",
                ctx.getCurrentKey(), product.getProductId(), product.getPrice());

            // Check if any order is waiting in the buffer
            Iterator<Map.Entry<String, Order>> orderIterator = orderBuffer.entries().iterator();

            if (orderIterator.hasNext()) {
                // Match found: emit result and clean up
                Map.Entry<String, Order> orderEntry = orderIterator.next();
                Order matchedOrder = orderEntry.getValue();

                LOG.info("[{}] Match found! Product {} matched with order {}",
                    ctx.getCurrentKey(), product.getProductId(), matchedOrder.getOrderId());

                out.collect(JoinExercise.JoinResult.matched(product, matchedOrder));

                // Remove matched order from buffer
                orderBuffer.remove(orderEntry.getKey());

                // Clear order timer state (timer will still fire but will be ignored)
                // Note: Flink doesn't support deleting registered timers, so we track validity in state
                if (!orderIterator.hasNext()) {
                    orderTimerState.clear();
                }
            } else {
                // No match: buffer product and register timeout timer
                LOG.info("[{}] No matching order found, buffering product {}",
                    ctx.getCurrentKey(), product.getProductId());

                productBuffer.put(product.getProductId(), product);

                // Register timer only if not already set for this key
                Long existingTimer = productTimerState.value();
                if (existingTimer == null) {
                    long timerTimestamp = ctx.timerService().currentWatermark() + timeoutMs;
                    ctx.timerService().registerEventTimeTimer(timerTimestamp);
                    productTimerState.update(timerTimestamp);

                    LOG.info("[{}] Registered product timeout timer at {}",
                        ctx.getCurrentKey(), timerTimestamp);
                }
            }
        }

        @Override
        public void processElement2(
                Order order,
                Context ctx,
                Collector<JoinExercise.JoinResult> out) throws Exception {

            LOG.info("[{}] Order received: {} with amount {}",
                ctx.getCurrentKey(), order.getOrderId(), order.getAmount());

            // Check if any product is waiting in the buffer
            Iterator<Map.Entry<String, Product>> productIterator = productBuffer.entries().iterator();

            if (productIterator.hasNext()) {
                // Match found: emit result and clean up
                Map.Entry<String, Product> productEntry = productIterator.next();
                Product matchedProduct = productEntry.getValue();

                LOG.info("[{}] Match found! Order {} matched with product {}",
                    ctx.getCurrentKey(), order.getOrderId(), matchedProduct.getProductId());

                out.collect(JoinExercise.JoinResult.matched(matchedProduct, order));

                // Remove matched product from buffer
                productBuffer.remove(productEntry.getKey());

                // Clear product timer state (timer will still fire but will be ignored)
                if (!productIterator.hasNext()) {
                    productTimerState.clear();
                }
            } else {
                // No match: buffer order and register timeout timer
                LOG.info("[{}] No matching product found, buffering order {}",
                    ctx.getCurrentKey(), order.getOrderId());

                orderBuffer.put(order.getOrderId(), order);

                // Register timer only if not already set for this key
                Long existingTimer = orderTimerState.value();
                if (existingTimer == null) {
                    long timerTimestamp = ctx.timerService().currentWatermark() + timeoutMs;
                    ctx.timerService().registerEventTimeTimer(timerTimestamp);
                    orderTimerState.update(timerTimestamp);

                    LOG.info("[{}] Registered order timeout timer at {}",
                        ctx.getCurrentKey(), timerTimestamp);
                }
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<JoinExercise.JoinResult> out) throws Exception {

            LOG.info("[{}] Timer fired at {}", ctx.getCurrentKey(), timestamp);

            // Check if this timer corresponds to a product timeout
            Long productTimer = productTimerState.value();
            if (productTimer != null && productTimer == timestamp) {
                LOG.info("[{}] Product timeout triggered", ctx.getCurrentKey());

                // Emit all buffered products as unmatched
                for (Product product : productBuffer.values()) {
                    LOG.info("[{}] Emitting unmatched product: {}",
                        ctx.getCurrentKey(), product.getProductId());
                    out.collect(JoinExercise.JoinResult.unmatchedProduct(product));
                }

                // Clear product buffer and timer state
                productBuffer.clear();
                productTimerState.clear();
            }

            // Check if this timer corresponds to an order timeout
            Long orderTimer = orderTimerState.value();
            if (orderTimer != null && orderTimer == timestamp) {
                LOG.info("[{}] Order timeout triggered", ctx.getCurrentKey());

                // Emit all buffered orders as unmatched
                for (Order order : orderBuffer.values()) {
                    LOG.info("[{}] Emitting unmatched order: {}",
                        ctx.getCurrentKey(), order.getOrderId());
                    out.collect(JoinExercise.JoinResult.unmatchedOrder(order));
                }

                // Clear order buffer and timer state
                orderBuffer.clear();
                orderTimerState.clear();
            }
        }
    }
}
