package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FileSinkExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    settings.getConfiguration().setString("parallelism.default", "4");
    // settings.getConfiguration().setString("table.exec.sink.keyed-shuffle", "FORCE");
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE GenData (
        order_id BIGINT,
        category STRING,
        amount DECIMAL(10, 2),
        product_id BIGINT,
        product_name STRING,
        `timestamp` TIMESTAMP(3)
      ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '2000000000',
        'fields.product_id.min' = '1',
        'fields.product_id.max' = '500'
      )
      """);

    tEnv.executeSql("""
      CREATE TABLE FileSink (
        order_id BIGINT,
        category STRING,
        amount DECIMAL(10, 2),
        product_id BIGINT,
        product_name STRING,
        `timestamp` TIMESTAMP(3),
        PRIMARY KEY (product_id) NOT ENFORCED
      ) PARTITIONED BY (product_id) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/flink/orders_output',
        'format' = 'csv',
        'sink.rolling-policy.rollover-interval' = '15 min'
      )
    """);

    // tEnv.executeSql("""
    // SELECT * FROM GenData
    // """).print();

    tEnv.executeSql("""
      INSERT INTO FileSink
      SELECT *
      FROM GenData
    """);
  }
}
