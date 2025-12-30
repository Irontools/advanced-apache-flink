package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableFunction2Example {
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

    Table orders = tEnv.from("Orders");

    orders
      .joinLateral(call(OrderAmountEnrichFunction.class, $("amount")))
      .select(
        $("order_id"),
        $("customer_name"),
        $("amount"),
        $("tier"),
        $("amount_difference"),
        $("tier_index")
      )
      // .printExplain();
      .execute()
      .print();
  }
}
