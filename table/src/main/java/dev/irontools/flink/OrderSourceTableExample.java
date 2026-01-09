package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;

public class OrderSourceTableExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE Orders (
        order_id STRING,
        customer_name STRING,
        category STRING,
        amount DECIMAL(10, 2),
        product_id STRING,
        product_name STRING,
        `timestamp` BIGINT
      ) WITH (
        'connector' = 'order-source',
        'totalCount' = '100'
      )
      """);

    // FOR CONNECTORS
    //
    // tEnv.executeSql("""
    //   CREATE TABLE FileSink (
    //     category STRING,
    //     total_amount DECIMAL(10, 2)
    //   ) WITH (
    //     'connector' = 'filesystem',
    //     'path' = 'file:///tmp/flink/orders_output',
    //     'format' = 'csv',
    //     'sink.rolling-policy.rollover-interval' = '15 sec'
    //   )
    // """);
    //
    // FOR CONNECTORS

    // tEnv.executeSql("""
    //   CREATE TABLE PrintSink (
    //     category STRING,
    //     total_amount DECIMAL(10, 2)
    //   ) WITH (
    //     'connector' = 'print'
    //   )
    // """);
    //
    // tEnv.executeSql("""
    //   COMPILE PLAN 'plan.json' FOR
    //   INSERT INTO PrintSink
    //   SELECT
    //     category,
    //     ROUND(SUM(amount), 2) as total_amount
    //   FROM Orders
    //   WHERE amount > 100.0
    //   GROUP BY category
    // """);

    // tEnv.loadPlan(PlanReference.fromFile("plan.json")).execute();

    // tEnv.executeSql("EXPLAIN SELECT * FROM Orders").print();

    tEnv.executeSql("""
      SELECT
        category,
        ROUND(SUM(amount), 2) as total_amount
      FROM Orders
      WHERE amount > 100.0
      GROUP BY category
      """).print();
  }
}
