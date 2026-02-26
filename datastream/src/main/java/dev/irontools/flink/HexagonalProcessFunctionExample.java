package dev.irontools.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Re-implementation of ProcessFunctionExample following hexagonal architecture (ports & adapters).
 *
 * Structure:
 *   DOMAIN LAYER   - WindowState, WindowResult, WindowStatePort, WindowAggregationService
 *                    No Flink imports. Pure business logic and interfaces.
 *   ADAPTER LAYER  - FlinkWindowStateAdapter (implements port via Flink ValueState)
 *   DRIVING ADAPTER - WindowedAggregationFunction (KeyedProcessFunction that drives the domain)
 *   INFRASTRUCTURE - main() wires sources, adapters, and sinks
 */
public class HexagonalProcessFunctionExample {

    private static final long WINDOW_SIZE_MS = 10_000L;

    // =====================================================================
    // DOMAIN LAYER — no Flink imports
    // =====================================================================

    // --- Domain Types ---

    public static class WindowState implements Serializable {
        private double sum;
        private int count;
        private long windowEnd;

        public WindowState() {}

        public WindowState(double sum, int count, long windowEnd) {
            this.sum = sum;
            this.count = count;
            this.windowEnd = windowEnd;
        }

        public double getSum() { return sum; }
        public void setSum(double sum) { this.sum = sum; }
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
        public long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
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

    /**
     * Returned by the domain service to tell the driving adapter what actions to take.
     * The domain cannot register timers or emit results directly — it returns decisions.
     */
    public static class AggregationDecision {
        private final WindowResult flushedResult;
        private final Long timerToRegister;

        public AggregationDecision(WindowResult flushedResult, Long timerToRegister) {
            this.flushedResult = flushedResult;
            this.timerToRegister = timerToRegister;
        }

        public WindowResult getFlushedResult() { return flushedResult; }
        public Long getTimerToRegister() { return timerToRegister; }
    }

    // --- Port ---

    public interface WindowStatePort extends Serializable {
        WindowState getState() throws Exception;
        void updateState(WindowState state) throws Exception;
        void clear() throws Exception;
    }

    // --- Domain Service ---

    /**
     * Pure business logic for tumbling window aggregation.
     * Depends only on the WindowStatePort interface — no Flink knowledge.
     */
    public static class WindowAggregationService implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(WindowAggregationService.class);

        private final WindowStatePort statePort;
        private final long windowSizeMs;

        public WindowAggregationService(WindowStatePort statePort, long windowSizeMs) {
            this.statePort = statePort;
            this.windowSizeMs = windowSizeMs;
        }

        /**
         * Process an incoming order. Returns a decision indicating:
         * - An optional flushed WindowResult (if a previous window was displaced)
         * - An optional timer target (if a new window was started)
         */
        public AggregationDecision processOrder(String category, double amount, long timestamp) throws Exception {
            long windowStart = getWindowStart(timestamp);
            long windowEnd = windowStart + windowSizeMs;

            WindowState current = statePort.getState();

            WindowResult flushed = null;
            Long timerTarget = null;

            if (current == null || current.getWindowEnd() != windowEnd) {
                if (current != null && current.getCount() > 0) {
                    flushed = buildResult(category, current);
                    LOG.info("[{}] Flushing previous window: [{}, {}) | Total: ${} | Count: {}",
                        category,
                        current.getWindowEnd() - windowSizeMs, current.getWindowEnd(),
                        String.format("%.2f", current.getSum()), current.getCount());
                }

                statePort.updateState(new WindowState(amount, 1, windowEnd));
                timerTarget = windowEnd;

                LOG.info("[{}] New window: [{}, {})", category, windowStart, windowEnd);
            } else {
                statePort.updateState(new WindowState(
                    current.getSum() + amount,
                    current.getCount() + 1,
                    windowEnd
                ));
            }

            WindowState updated = statePort.getState();
            LOG.info("[{}] Added order: ${} | Window total: ${} ({} orders)",
                category,
                String.format("%.2f", amount),
                String.format("%.2f", updated.getSum()),
                updated.getCount());

            return new AggregationDecision(flushed, timerTarget);
        }

        /**
         * Called when a window timer fires. Returns the aggregated result if the window matches.
         */
        public Optional<WindowResult> emitWindow(String category, long timerTimestamp) throws Exception {
            WindowState state = statePort.getState();
            if (state != null && state.getWindowEnd() == timerTimestamp && state.getCount() > 0) {
                WindowResult result = buildResult(category, state);
                statePort.clear();

                LOG.info("[{}] Emitting window result: [{}, {}) | Total: ${} | Count: {}",
                    category,
                    timerTimestamp - windowSizeMs, timerTimestamp,
                    String.format("%.2f", state.getSum()), state.getCount());

                return Optional.of(result);
            }
            statePort.clear();
            return Optional.empty();
        }

        private long getWindowStart(long timestamp) {
            return timestamp - (timestamp % windowSizeMs);
        }

        private WindowResult buildResult(String category, WindowState state) {
            long windowStart = state.getWindowEnd() - windowSizeMs;
            return new WindowResult(category, windowStart, state.getWindowEnd(),
                state.getSum(), state.getCount());
        }
    }

    // =====================================================================
    // ADAPTER LAYER — Flink-specific implementations of ports
    // =====================================================================

    public static class FlinkWindowStateAdapter implements WindowStatePort {
        private transient ValueState<WindowState> state;

        public void initializeState(org.apache.flink.api.common.functions.RuntimeContext ctx) {
            ValueStateDescriptor<WindowState> descriptor =
                new ValueStateDescriptor<>("window-state", TypeInformation.of(WindowState.class));
            this.state = ctx.getState(descriptor);
        }

        @Override
        public WindowState getState() throws Exception {
            return state.value();
        }

        @Override
        public void updateState(WindowState windowState) throws Exception {
            state.update(windowState);
        }

        @Override
        public void clear() throws Exception {
            state.clear();
        }
    }

    // =====================================================================
    // DRIVING ADAPTER — Flink operator that drives the domain service
    // =====================================================================

    /**
     * Thin Flink operator. Its only responsibilities are:
     *  1. Initialize adapters and wire the domain service (open)
     *  2. Delegate order processing to the domain service (processElement)
     *  3. Act on decisions: register timers, collect results
     *  4. Delegate timer handling to the domain service (onTimer)
     */
    public static class WindowedAggregationFunction
            extends KeyedProcessFunction<String, Order, WindowResult> {

        private static final Logger LOG = LoggerFactory.getLogger(WindowedAggregationFunction.class);

        private transient WindowAggregationService aggregationService;

        @Override
        public void open(OpenContext openContext) throws Exception {
            FlinkWindowStateAdapter stateAdapter = new FlinkWindowStateAdapter();
            stateAdapter.initializeState(getRuntimeContext());

            aggregationService = new WindowAggregationService(stateAdapter, WINDOW_SIZE_MS);
        }

        @Override
        public void processElement(
                Order order, Context ctx, Collector<WindowResult> out) throws Exception {

            AggregationDecision decision = aggregationService.processOrder(
                ctx.getCurrentKey(), order.getAmount(), ctx.timestamp()
            );

            if (decision.getFlushedResult() != null) {
                out.collect(decision.getFlushedResult());
            }

            if (decision.getTimerToRegister() != null) {
                ctx.timerService().registerEventTimeTimer(decision.getTimerToRegister());
                LOG.info("[{}] Timer registered for {}", ctx.getCurrentKey(), decision.getTimerToRegister());
            }
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<WindowResult> out) throws Exception {

            LOG.info("[{}] Timer fired at {}", ctx.getCurrentKey(), timestamp);

            aggregationService.emitWindow(ctx.getCurrentKey(), timestamp)
                .ifPresent(out::collect);
        }
    }

    // =====================================================================
    // INFRASTRUCTURE — wires sources, operators, and sinks
    // =====================================================================

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Iterable<Order> orders = OrderGenerator.generateOrdersWithDelay(100, 10, 5000, false);

        env.fromCollection(orders.iterator(), Order.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getTimestamp())
            )
            .keyBy(Order::getCategory)
            .process(new WindowedAggregationFunction())
            .print();

        env.execute("Hexagonal ProcessFunction Window Aggregation Example");
    }
}
