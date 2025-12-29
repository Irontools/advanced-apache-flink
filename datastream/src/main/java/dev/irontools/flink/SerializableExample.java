package dev.irontools.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.List;

/**
 * This example shows the CORRECT way to use stateful objects in Flink
 * by making them serializable.
 */
public class SerializableExample {

    /**
     * A properly serializable class that implements Serializable.
     * All fields must also be serializable (primitives and Strings are).
     */
    static class PriceCalculator implements Serializable {
        private static final long serialVersionUID = 1L;

        private final double taxRate;
        private final String region;

        public PriceCalculator(double taxRate, String region) {
            this.taxRate = taxRate;
            this.region = region;
        }

        public double calculateTotal(double amount) {
            return amount * (1 + taxRate);
        }

        public String getRegion() {
            return region;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Order> orders = OrderGenerator.generateOrders(10);

        // Create a serializable object - this will work!
        PriceCalculator calculator = new PriceCalculator(0.08, "US-CA");

        // This works because PriceCalculator implements Serializable
        // Another approach: create a RichMapFunction (or ProcessFunction) and save PriceCalculator
        // as a transient field initialized in the open() method.
        env.fromData(orders)
            .filter(order -> order.getAmount() > 50.0)
            .map(order -> {
                double totalWithTax = calculator.calculateTotal(order.getAmount());
                return String.format("Order %s: $%.2f (with tax in %s: $%.2f)",
                    order.getOrderId(),
                    order.getAmount(),
                    calculator.getRegion(),
                    totalWithTax);
            })
            .print();

        env.execute("Serializable Example - Works!");
    }
}
