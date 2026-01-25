package dev.irontools.flink.workshop.join;

import dev.irontools.flink.Order;
import dev.irontools.flink.Product;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JoinTest {

    private static final long TIMEOUT_MS = 5000L;
    private KeyedTwoInputStreamOperatorTestHarness<String, Product, Order, JoinExercise.JoinResult> testHarness;

    @BeforeEach
    public void setup() throws Exception {
        JoinSolution.BufferedJoinFunction joinFunction =
            new JoinSolution.BufferedJoinFunction(TIMEOUT_MS);

        // Wrap it in a KeyedCoProcessOperator
        KeyedCoProcessOperator<String, Product, Order, JoinExercise.JoinResult> operator =
            new KeyedCoProcessOperator<>(joinFunction);

        // Create test harness with key selectors for both streams
        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
            operator,
            Product::getProductId,  // key selector for first stream (products)
            Order::getProductId,     // key selector for second stream (orders)
            TypeInformation.of(String.class)
        );

        // Open the test harness (initializes state)
        testHarness.open();
    }

    @Test
    public void testProductArrivesBeforeOrder() throws Exception {
        // Step 1: Process product at timestamp 1000ms
        Product product = new Product("P1", "Laptop", 100.0, 1000L);
        testHarness.processElement1(product, 1000L);

        // Step 2: Verify product is buffered (no output yet)
        assertEquals(0, testHarness.getOutput().size(),
            "Product should be buffered, no output expected yet");

        // Step 3: Process order at timestamp 2000ms (after product)
        Order order = new Order(
            "O1",
            "John Doe",
            "Electronics",
            100.0,
            "P1",
            "Laptop",
            2000L
        );
        testHarness.processElement2(order, 2000L);

        // Step 4: Verify matched result is emitted
        assertEquals(1, testHarness.getOutput().size(),
            "One matched result should be emitted");

        // Extract the result from StreamRecord
        @SuppressWarnings("unchecked")
        StreamRecord<JoinExercise.JoinResult> record =
            (StreamRecord<JoinExercise.JoinResult>) testHarness.getOutput().poll();

        assertNotNull(record, "StreamRecord should not be null");
        JoinExercise.JoinResult result = record.getValue();

        assertNotNull(result, "Result should not be null");
        assertEquals(JoinExercise.JoinResult.JoinType.MATCHED, result.getType(),
            "Result type should be MATCHED");
        assertEquals("P1", result.getProduct().getProductId(),
            "Product ID should match");
        assertEquals("O1", result.getOrder().getOrderId(),
            "Order ID should match");
        assertEquals(100.0, result.getProduct().getPrice(),
            "Product price should be 100.0");
        assertEquals(100.0, result.getOrder().getAmount(),
            "Order amount should be 100.0");
    }

    @Disabled("Not implemented yet")
    @Test
    public void testOrderArrivesBeforeProduct() throws Exception {

    }

    @Disabled("Not implemented yet")
    @Test
    public void testProductTimeout() throws Exception {

    }

    @Disabled("Not implemented yet")
    @Test
    public void testOrderTimeout() throws Exception {

    }

    @Disabled("Not implemented yet")
    @Test
    public void testMultipleMatchesForSameKey() throws Exception {

    }
}
