package dev.irontools.flink.workshop.join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Solution: Join between products and orders using Process Table Function.
 *
 * This implementation demonstrates:
 * 1. Multi-table PTF invocation with consistent partitioning
 * 2. Timeout-based join semantics (10 second window)
 * 3. MapView state for efficient buffering
 * 4. Timer-based cleanup and unmatched record emission
 *
 * Key differences from DataStream approach:
 * - Function Base: ProcessTableFunction vs KeyedCoProcessFunction
 * - Input Handling: eval() with null-checking vs processElement1()/processElement2()
 * - State: MapView with @StateHint vs MapStateDescriptor in open()
 * - Partitioning: PARTITION BY in SQL vs keyBy() in DataStream
 * - Timers: registerOnTime(name, instant) vs registerEventTimeTimer(timestamp)
 * - Data Sources: DDL with connectors vs fromCollection() + generators
 */
public class JoinSolution {

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

        // Register the JoinPTF function
        tEnv.createTemporaryFunction("join_ptf", JoinSolutionPTF.class);

        // Execute join query
        // Both tables partition by product_id (required for PTF multi-table input)
        // Timeout is configured via constructor (10 seconds by default)
        tEnv.executeSql("""
            SELECT
                join_type,
                product_id,
                product_name,
                price,
                order_id,
                customer_name,
                amount
            FROM join_ptf(
                `product` => TABLE Products PARTITION BY product_id,
                `order` => TABLE Orders PARTITION BY product_id,
                `on_time` => DESCRIPTOR(event_time),
                `uid` => 'join_ptf'
            )
            """).print();
    }
}
