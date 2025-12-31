package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ProcessTableFunctionExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.createTemporaryFunction("order_session_processor", OrderSessionProcessor.class);

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
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
      ) WITH (
        'connector' = 'order-source',
        'totalCount' = '100'
      )
      """);

    tEnv.executeSql("""
      SELECT
        session_type,
        customer_name,
        order_count,
        total_amount,
        session_start,
        session_duration
      FROM order_session_processor(
        `order` => TABLE Orders PARTITION BY customer_name,
        `on_time` => DESCRIPTOR(event_time),
        `sessionTimeoutSec` => 30,
        `reminderOffsetSec` => 10
      )
      """).print();
  }
}
