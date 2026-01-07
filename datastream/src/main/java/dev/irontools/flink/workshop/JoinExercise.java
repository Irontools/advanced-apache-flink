package dev.irontools.flink.workshop;

import dev.irontools.flink.Order;
import dev.irontools.flink.OrderGenerator;
import dev.irontools.flink.Product;
import dev.irontools.flink.ProductGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Exercise: Implement a join between orders and products.
 *
 * Emit JoinResult with matched pairs or unmatched records. Records should only match
 * if they arrive within TIMEOUT_MS of each other.
 *
 * When you're ready to test, update JoinTest with your implementation. Implement more unit tests.
 *
 * Keep scrolling to the bottom for hints!
 *
 * Bonus points:
 * - Emit a new type of JoinResult for pending unmatched records (e.g. at time TIMEOUT_MS / 2)
 * - Emit metrics
 */
public class JoinExercise {

    private static final Logger LOG = LoggerFactory.getLogger(JoinExercise.class);
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

        // TODO: implement the join

        env.execute("Join Exercise");
    }

    /**
     * Result of the buffered join operation.
     * Can represent matched pairs or unmatched records.
     */
    public static class JoinResult {
        private final JoinType type;
        private final Product product;
        private final Order order;

        private JoinResult(JoinType type, Product product, Order order) {
            this.type = type;
            this.product = product;
            this.order = order;
        }

        public static JoinResult matched(Product product, Order order) {
            return new JoinResult(JoinType.MATCHED, product, order);
        }

        public static JoinResult unmatchedProduct(Product product) {
            return new JoinResult(JoinType.UNMATCHED_PRODUCT, product, null);
        }

        public static JoinResult unmatchedOrder(Order order) {
            return new JoinResult(JoinType.UNMATCHED_ORDER, null, order);
        }

        public JoinType getType() {
            return type;
        }

        public Product getProduct() {
            return product;
        }

        public Order getOrder() {
            return order;
        }

        @Override
        public String toString() {
            switch (type) {
                case MATCHED:
                    return String.format(
                        "JoinResult{MATCHED: product=%s, order=%s, price=%.2f, amount=%.2f}",
                        product.getProductId(), order.getOrderId(),
                        product.getPrice(), order.getAmount()
                    );
                case UNMATCHED_PRODUCT:
                    return String.format(
                        "JoinResult{UNMATCHED_PRODUCT: product=%s, price=%.2f}",
                        product.getProductId(), product.getPrice()
                    );
                case UNMATCHED_ORDER:
                    return String.format(
                        "JoinResult{UNMATCHED_ORDER: order=%s, amount=%.2f}",
                        order.getOrderId(), order.getAmount()
                    );
                default:
                    return "JoinResult{UNKNOWN}";
            }
        }

        public enum JoinType {
            MATCHED,
            UNMATCHED_PRODUCT,
            UNMATCHED_ORDER
        }
    }
}
























































/*** Hint 1
 *
 * Use KeyedCoProcessFunction and key both streams by productId.
 */
































































/*** Hint 2
 *
 * Inspect org.apache.flink.streaming.api.operators.co.IntervalJoinOperator
 */
