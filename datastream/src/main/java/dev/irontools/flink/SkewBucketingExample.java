package dev.irontools.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class SkewBucketingExample {
  private final static int NUM_BUCKETS = 32;

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    conf.setString("execution.checkpointing.interval", "10s");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

    Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(5000000, 5, 5000, false);

    DataStream<Order> ordersStream =
        env.fromCollection(orders.iterator(), Order.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                      @Override
                      public long extractTimestamp(Order element, long recordTimestamp) {
                        return element.getTimestamp();
                      }
                    })
            );

    DataStream<OrderSalted> saltedStream =
        ordersStream.map(new MapFunction<Order, OrderSalted>() {
          @Override
          public OrderSalted map(Order o) {
            int bucket = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
            String saltedKey = o.getCategory() + "#" + bucket;
            return new OrderSalted(
                saltedKey,
                o.getCategory(),
                o.getAmount(),
                o.getTimestamp()
            );
          }
        });

    DataStream<CategoryAmount> partialStream =
        saltedStream
            .keyBy(OrderSalted::saltedCategory)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .reduce(new ReduceFunction<OrderSalted>() {
              @Override
              public OrderSalted reduce(OrderSalted a, OrderSalted b) {
                return new OrderSalted(
                    a.saltedCategory(),
                    a.category(),
                    a.amount() + b.amount(),
                    Math.max(a.timestamp(), b.timestamp())
                );
              }
            })
            .map(new MapFunction<OrderSalted, CategoryAmount>() {
              @Override
              public CategoryAmount map(OrderSalted v) {
                return new CategoryAmount(v.category(), v.amount(), v.timestamp());
              }
            });

    DataStream<CategoryAmount> resultStream =
        partialStream
            .keyBy(CategoryAmount::category)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .reduce(new ReduceFunction<CategoryAmount>() {
              @Override
              public CategoryAmount reduce(CategoryAmount a, CategoryAmount b) {
                return new CategoryAmount(
                    a.category(),
                    a.amount() + b.amount(),
                    Math.max(a.timestamp(), b.timestamp())
                );
              }
            });

    resultStream.print();

    env.execute("Flink Data Skew Mitigation with Bucketing");
  }

  public record OrderSalted(String saltedCategory, String category, Double amount, Long timestamp) {}

  public record CategoryAmount(String category, Double amount, Long timestamp) {}
}
