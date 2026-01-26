package dev.irontools.flink.workshop.upgrade;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Exercise: perform stateful upgrade for a Flink SQL job.
 *
 * The goal is add a where clause to filter out low-value orders (amount < 50).
 * This change has to made WITHOUT LOSING STATE. So, the high-level steps are:
 * - Start a job
 * - Take a savepoint
 *    - The easiest way to take a savepoint: curl -X POST http://localhost:$job_port/jobs/$job_id/savepoints
 * - Start a new version of the job (with the where clause) from the savepoint
 *
 * Keep scrolling to the bottom for hints!
 */
public class UpgradeExercise {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    settings.getConfiguration().setString("execution.checkpointing.interval", "10s");
    settings.getConfiguration().setString("execution.checkpointing.savepoint-dir", "file:///tmp/flink-savepoints");
    // settings.getConfiguration().setString("execution.state-recovery.path", "file:///tmp/flink-savepoints/your-savepoint-dir");
    TableEnvironment tEnv = TableEnvironment.create(settings);

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
        'totalCount' = '10000',
        'batchSize' = '10',
        'delayMillis' = '5000'
      )
      """);

    tEnv.executeSql(
        """
      CREATE TABLE PrintSink (
        category STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_amount DECIMAL(10, 2),
        order_count BIGINT,
        PRIMARY KEY (category, window_start) NOT ENFORCED
      ) WITH (
        'connector' = 'print'
      )
    """);

    // tEnv.loadPlan(PlanReference.fromFile("plan.json")).execute();

    tEnv.executeSql(
      """
      INSERT INTO PrintSink
      SELECT
          category,
          window_start,
          window_end,
          SUM(amount) AS total_amount,
          COUNT(*) AS order_count
      FROM TABLE(
          TUMBLE(TABLE Orders, DESCRIPTOR(event_time), INTERVAL '15' SECONDS)
      )
      GROUP BY
          category,
          window_start,
          window_end
      """)
        .print();
  }
}






















































/*** Hint 1
 * You must leverage compiled plans to perform a stateful upgrade.
 */





























































/*** Hint 2
 * Don't take a savepoint for the job running from SQL. Instead, create a compiled plan for the job,
 * start the job from it, and then take a savepoint. This ensures stable UID assignment.
 */




























































/*** Hint 3
 * Create a compiled plan not just for the original job, but also for the upgraded one (with the where clause).
 * You can do it without the savepoint recovery. The goal is to get both plans and COMPARE them.
 */
