package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TableFunctionExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.createTemporaryFunction("split_tiers", CategoryTierFunction.class);

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

    tEnv.executeSql("""
      SELECT
        o.order_id,
        o.customer_name,
        o.category,
        o.amount,
        t.tier_name,
        t.tier_min,
        t.tier_max,
        t.is_in_tier
      FROM Orders AS o
      CROSS JOIN LATERAL TABLE(split_tiers(o.amount)) AS t(tier_name, tier_min, tier_max, is_in_tier)
      """).print();
  }
}
