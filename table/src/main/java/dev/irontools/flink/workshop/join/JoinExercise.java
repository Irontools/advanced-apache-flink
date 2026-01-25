package dev.irontools.flink.workshop.join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Exercise: Implement a join between products and orders using Process Table Function.
 *
 * Emit join results with matched pairs or unmatched records. Records should only match
 * if they arrive within 10 seconds of each other.
 *
 * When you're ready to test, update JoinTest with your implementation. Implement more unit tests.
 *
 * Keep scrolling to the bottom for hints!
 *
 * Bonus points:
 * - Emit a new type of JoinResult for pending unmatched records (e.g. at 5 seconds)
 * - Emit metrics for join performance
 */
public class JoinExercise {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create Products table with 5 products, 2 update cycles, 2 second delay
        // appendOnly = true ensures INSERT-only mode (required for PTFs with time semantics)
        tEnv.executeSql("""
            CREATE TABLE Products (
                product_id STRING,
                product_name STRING,
                price DECIMAL(10, 2),
                update_time BIGINT,
                event_time AS TO_TIMESTAMP_LTZ(update_time, 3),
                WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
            ) WITH (
                'connector' = 'product-source',
                'productCount' = '5',
                'updateCycles' = '2',
                'delayMillis' = '2000',
                'appendOnly' = 'true'
            )
            """);

        // Create Orders table with 30 orders, 3 second delay
        tEnv.executeSql("""
            CREATE TABLE Orders (
                order_id STRING,
                customer_name STRING,
                category STRING,
                amount DECIMAL(10, 2),
                product_id STRING,
                product_name STRING,
                `timestamp` BIGINT,
                event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
            ) WITH (
                'connector' = 'order-source',
                'totalCount' = '30',
                'batchSize' = '1',
                'delayMillis' = '3000'
            )
            """);

        // TODO: implement and register the JoinPTF function
        // tEnv.createTemporaryFunction("join_ptf", JoinPTF.class);

        // TODO: implement and execute the join query
        // tEnv.executeSql("""
        //     SELECT
        //         join_type,
        //         product_id,
        //         product_name,
        //         price,
        //         order_id,
        //         customer_name,
        //         amount
        //     FROM ???
        //     """).print();
    }
}















































/*** Hint 1
 *
 * Use ProcessTableFunction with multiple table inputs.
 * Check PTF docs Multiple Tables section:
 * https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/table/functions/ptfs/
 */





















































/*** Hint 2
 *
 * Both tables must PARTITION BY the same key (product_id).
 */















































/*** Hint 3
 *
 * In eval(), use null-checking to distinguish which table sent the row.
 */














































/*** Hint 4
 *
 * Reference OrderSessionProcessor for timer patterns and state management.
 */














































/*** Hint 5
 *
 * Use MapView state (from org.apache.flink.table.api.dataview package) to buffer
 * products (by productId) and orders (by orderId).
 * See PTF docs Large State section.
 */
