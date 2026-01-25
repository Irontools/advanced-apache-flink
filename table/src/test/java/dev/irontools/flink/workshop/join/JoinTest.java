package dev.irontools.flink.workshop.join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for JoinPTF.
 *
 * Note: PTF testing is more challenging than DataStream testing because there's no
 * equivalent to KeyedTwoInputStreamOperatorTestHarness. These tests use a full
 * TableEnvironment with in-memory sources, making them integration tests rather than
 * pure unit tests.
 *
 * Testing approach:
 * 1. Create tables with deterministic in-memory data
 * 2. Register JoinPTF function
 * 3. Execute join query
 * 4. Collect and verify results
 *
 * Challenges:
 * - Time control is limited (no manual watermark advancement)
 * - Bounded sources must be used for deterministic behavior
 * - Timeout testing requires actual time delays or very short timeout values
 */
public class JoinTest {

    private TableEnvironment tEnv;

    @BeforeEach
    public void setup() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryFunction("join_ptf", JoinSolutionPTF.class);
    }

    /**
     * Test scenario: Product arrives before matching order.
     * Expected: MATCHED result emitted when order arrives.
     */
    @Test
    public void testProductArrivesBeforeOrder() throws Exception {
        // Create Products table with minimal data - 1 product, 0 update cycles, 100ms delay
        tEnv.executeSql(
            "CREATE TABLE Products (" +
            "    product_id STRING," +
            "    product_name STRING," +
            "    price DECIMAL(10, 2)," +
            "    update_time BIGINT," +
            "    event_time AS TO_TIMESTAMP_LTZ(update_time, 3)," +
            "    WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND" +
            ") WITH (" +
            "    'connector' = 'product-source'," +
            "    'productCount' = '1'," +
            "    'updateCycles' = '0'," +
            "    'delayMillis' = '100'," +
            "    'appendOnly' = 'true'" +
            ")"
        );

        // Create Orders table with minimal data - 1 order, 200ms delay (arrives after product)
        tEnv.executeSql(
            "CREATE TABLE Orders (" +
            "    order_id STRING," +
            "    customer_name STRING," +
            "    category STRING," +
            "    amount DECIMAL(10, 2)," +
            "    product_id STRING," +
            "    product_name STRING," +
            "    `timestamp` BIGINT," +
            "    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
            "    WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND" +
            ") WITH (" +
            "    'connector' = 'order-source'," +
            "    'totalCount' = '1'," +
            "    'batchSize' = '1'," +
            "    'delayMillis' = '200'" +
            ")"
        );

        // Execute join query (timeout is hardcoded in PTF at 10 seconds)
        Table resultTable = tEnv.sqlQuery(
            "SELECT " +
            "    join_type, " +
            "    product_id, " +
            "    product_name, " +
            "    price, " +
            "    order_id, " +
            "    customer_name, " +
            "    amount " +
            "FROM join_ptf( " +
            "    `product` => TABLE Products PARTITION BY product_id, " +
            "    `order` => TABLE Orders PARTITION BY product_id, " +
            "    `on_time` => DESCRIPTOR(event_time), " +
            "    `uid` => 'join_ptf_test' " +
            ")"
        );

        // Collect results
        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = resultTable.execute().collect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        // Verify: should have exactly 1 MATCHED result
        assertEquals(1, results.size(), "Should emit exactly one result");

        Row result = results.get(0);
        assertEquals("MATCHED", result.getField(0), "Result type should be MATCHED");
        assertNotNull(result.getField(1), "Product ID should not be null");
        assertNotNull(result.getField(2), "Product name should not be null");
        assertNotNull(result.getField(3), "Price should not be null");
        assertNotNull(result.getField(4), "Order ID should not be null");
        assertNotNull(result.getField(5), "Customer name should not be null");
        assertNotNull(result.getField(6), "Amount should not be null");
    }

    @Disabled("Implement this test")
    @Test
    public void testOrderArrivesBeforeProduct() {

    }

    @Disabled("Implement this test")
    @Test
    public void testProductTimeout() {

    }

    @Disabled("Implement this test")
    @Test
    public void testOrderTimeout() {

    }

    @Disabled("Implement this test")
    @Test
    public void testMultipleOrdersSameProduct() {

    }
}
