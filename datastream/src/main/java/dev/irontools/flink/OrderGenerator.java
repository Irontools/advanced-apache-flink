package dev.irontools.flink;

import com.github.javafaker.Faker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class OrderGenerator {
    private static final Faker faker = new Faker();
    private static final Random random = new Random();
    private static final String[] CATEGORIES = {
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports"
    };

    public static List<Order> generateOrders(int count) {
        List<Order> orders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            orders.add(generateOrder());
        }
        return orders;
    }

    public static Order generateOrder() {
        String orderId = faker.idNumber().valid();
        String customerName = faker.name().fullName();
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        Double amount = 10.0 + (random.nextDouble() * 990.0);
        String productName = faker.commerce().productName();
        Long timestamp = System.currentTimeMillis();

        return new Order(orderId, customerName, category, amount, productName, timestamp);
    }

    /**
     * Generate an iterable of orders with delays between batches.
     * This is useful for simulating a streaming source that produces data over time.
     *
     * @param totalCount Total number of orders to generate
     * @param batchSize Number of orders to generate in each batch
     * @param delayMillis Delay in milliseconds between batches
     * @return Iterable of orders
     */
    public static Iterable<Order> generateOrdersWithDelay(int totalCount, int batchSize, long delayMillis) {
        return () -> new DelayedOrderIterator(totalCount, batchSize, delayMillis);
    }

    /**
     * Iterator that generates orders with delays between batches.
     */
    public static class DelayedOrderIterator implements Iterator<Order>, Serializable {
        private final int totalCount;
        private final int batchSize;
        private final long delayMillis;
        private int generatedCount = 0;
        private int currentBatchCount = 0;

        public DelayedOrderIterator(int totalCount, int batchSize, long delayMillis) {
            this.totalCount = totalCount;
            this.batchSize = batchSize;
            this.delayMillis = delayMillis;
        }

        @Override
        public boolean hasNext() {
            return generatedCount < totalCount;
        }

        @Override
        public Order next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }

            // Pause between batches (but not before the first batch)
            if (currentBatchCount == 0 && generatedCount > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while pausing", e);
                }
            }

            Order order = generateOrder();
            generatedCount++;
            currentBatchCount++;

            // Reset batch counter when batch is complete
            if (currentBatchCount >= batchSize) {
                currentBatchCount = 0;
            }

            return order;
        }
    }
}
