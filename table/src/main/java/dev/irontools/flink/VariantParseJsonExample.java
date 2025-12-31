package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class VariantParseJsonExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE VIEW json_events (json_string) AS VALUES
        ('{"customer": "Bob", "amount": 100.50}'),
        ('{"name": "Alice", "items": [1, 2, 3], "premium": true}'),
        ('{"order_id": "ORD-001", "metadata": {"region": "US", "date": "2024-01-15"}}'),
        ('{"price": 99.99, "active": false}')
      """);

    tEnv.executeSql("""
      CREATE TABLE variant_sink (
        parsed_data VARIANT
      ) WITH (
        'connector' = 'print'
      )
      """);

    tEnv.executeSql("""
      INSERT INTO variant_sink
      SELECT parse_json(json_string) AS parsed_data
      FROM json_events
      """);
  }
}
