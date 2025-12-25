package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ScalarFunctionExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.createTemporaryFunction("xxhash", XxHashFunction.class);

    tEnv.executeSql("""
      CREATE TABLE Orders (
        order_id STRING,
        customer_name STRING,
        category STRING,
        amount DECIMAL(10, 2),
        product_name STRING,
        `timestamp` BIGINT
      ) WITH (
        'connector' = 'order-source',
        'totalCount' = '100'
      )
      """);

    tEnv.executeSql("""
      SELECT
        customer_name,
        xxhash(customer_name) AS customer_hash
      FROM Orders
      """).print();
  }
}
