package dev.irontools.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Example demonstrating KeyedProcessFunction for implementing custom windowed aggregations.
 *
 * This example shows how to manually implement a tumbling window using:
 * - ValueState to maintain window state (aggregated values and buffered elements)
 * - Timers to trigger window emission at window boundaries
 *
 * The implementation aggregates orders by category in 10-second tumbling windows
 */
public class ProcessFunctionExample {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessFunctionExample.class);

    private static final long WINDOW_SIZE_MS = 10_000L;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate 100 orders with pauses: 10 orders per batch, 5-second delay between batches
        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(100, 10, 5000);

        env.fromCollection(orders.iterator(), Order.class)
            // Assign timestamps and watermarks for event time processing
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            )
            .keyBy(Order::getCategory)
            .process(new WindowedAggregationFunction())
            .print();

        env.execute("ProcessFunction Window Aggregation Example");
    }

    /**
     * Custom KeyedProcessFunction that implements tumbling window aggregation.
     *
     * 1. Buffers elements in state until the window boundary
     * 2. Registers a timer for the window end time
     * 3. Emits aggregated results when the timer fires
     */
    public static class WindowedAggregationFunction
            extends KeyedProcessFunction<String, Order, WindowResult> {

        // State to store accumulated sum for the current window
        private transient ValueState<Double> windowSumState;

        // State to store the count of orders in the current window
        private transient ValueState<Integer> windowCountState;

        // State to track the current window end time
        private transient ValueState<Long> windowEndState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Double> sumDescriptor =
                new ValueStateDescriptor<>("window-sum", TypeInformation.of(Double.class));
            windowSumState = getRuntimeContext().getState(sumDescriptor);

            ValueStateDescriptor<Integer> countDescriptor =
                new ValueStateDescriptor<>("window-count", TypeInformation.of(Integer.class));
            windowCountState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Long> windowEndDescriptor =
                new ValueStateDescriptor<>("window-end", TypeInformation.of(Long.class));
            windowEndState = getRuntimeContext().getState(windowEndDescriptor);
        }

        @Override
        public void processElement(
                Order order,
                Context ctx,
                Collector<WindowResult> out) throws Exception {

            long timestamp = ctx.timestamp();

            // Calculate which window this element belongs to
            long windowStart = getWindowStart(timestamp);
            long windowEnd = windowStart + WINDOW_SIZE_MS;

            // Get or initialize the current window end time
            Long currentWindowEnd = windowEndState.value();

            // If this is a new window, initialize state and register a timer
            if (currentWindowEnd == null || currentWindowEnd != windowEnd) {
                // If we have data from a previous window that wasn't triggered yet,
                // emit it now (handles out-of-order data)
                if (currentWindowEnd != null && windowSumState.value() != null) {
                    emitWindow(ctx.getCurrentKey(), currentWindowEnd, out);
                    clearState();
                }

                // Set up new window
                windowEndState.update(windowEnd);
                windowSumState.update(0.0);
                windowCountState.update(0);

                // Register timer for window boundary
                TimerService timerService = ctx.timerService();
                timerService.registerEventTimeTimer(windowEnd);

                LOG.info(
                    "[{}] New window registered: [{}, {}), timer set for {}",
                    ctx.getCurrentKey(), windowStart,
                    windowEnd, windowEnd
                );
            }

            // Update aggregation state (sum and count)
            Double currentSum = windowSumState.value();
            Integer currentCount = windowCountState.value();

            windowSumState.update(currentSum + order.getAmount());
            windowCountState.update(currentCount + 1);

            LOG.info(
                "[{}] Added order: ${} | Window total: ${} ({} orders)",
                ctx.getCurrentKey(),
                String.format("%.2f", order.getAmount()),
                String.format("%.2f", windowSumState.value()),
                windowCountState.value()
            );
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<WindowResult> out) throws Exception {

            // Timer fires at window boundary
            LOG.info(
                "[{}] Timer fired at {} (window end time)",
                ctx.getCurrentKey(), timestamp
            );

            // Emit the window results
            Long windowEnd = windowEndState.value();
            if (windowEnd != null && windowEnd == timestamp) {
                emitWindow(ctx.getCurrentKey(), timestamp, out);

                // Clear state after emission
                clearState();
            }
        }

        /**
         * Emit the aggregated window result.
         */
        private void emitWindow(
                String category,
                long windowEnd,
                Collector<WindowResult> out) throws Exception {

            Double sum = windowSumState.value();
            Integer count = windowCountState.value();

            if (sum != null && count != null && count > 0) {
                long windowStart = windowEnd - WINDOW_SIZE_MS;
                WindowResult result = new WindowResult(
                    category,
                    windowStart,
                    windowEnd,
                    sum,
                    count
                );

                LOG.info(
                    "[{}] Emitting window result: [{}, {}) | Total: ${} | Count: {}",
                    category, windowStart, windowEnd, String.format("%.2f", sum), count
                );

                out.collect(result);
            }
        }

        /**
         * Clear all state for the current window.
         */
        private void clearState() throws Exception {
            windowSumState.clear();
            windowCountState.clear();
            windowEndState.clear();
        }

        /**
         * Calculate the window start time for a given timestamp.
         * Implements tumbling window logic: windowStart = timestamp - (timestamp % windowSize)
         */
        private long getWindowStart(long timestamp) {
            return timestamp - (timestamp % WINDOW_SIZE_MS);
        }
    }

    public static class WindowResult {
        private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

        private final String category;
        private final long windowStart;
        private final long windowEnd;
        private final double totalAmount;
        private final int orderCount;

        public WindowResult(String category, long windowStart, long windowEnd,
                          double totalAmount, int orderCount) {
            this.category = category;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.totalAmount = totalAmount;
            this.orderCount = orderCount;
        }

        private String formatTime(long timestamp) {
            return TIME_FORMATTER.format(Instant.ofEpochMilli(timestamp));
        }

        @Override
        public String toString() {
            return String.format(
                "WindowResult{category='%s', window=[%s, %s), total=$%.2f, count=%d}",
                category, formatTime(windowStart), formatTime(windowEnd), totalAmount, orderCount
            );
        }
    }
}
