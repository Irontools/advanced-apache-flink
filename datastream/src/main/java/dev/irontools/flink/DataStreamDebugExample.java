package dev.irontools.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamDebugExample {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("execution.checkpointing.savepoint-dir", "file:///tmp/flink-savepoints");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(5000000, 5, 5000, false);

        env.fromCollection(orders.iterator(), Order.class)
            .filter(order -> order.getAmount() > 100.0)
            .map(new MapFunction<Order, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(Order order) throws Exception {
                    return Tuple2.of(order.getCategory(), order.getAmount());
                }
            })
            .keyBy(t -> t.f0)
            .sum("f1")
            .print();

        env.execute("DataStream Example with Orders");
    }
}
