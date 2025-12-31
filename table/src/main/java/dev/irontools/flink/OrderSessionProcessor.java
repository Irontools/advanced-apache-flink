package dev.irontools.flink;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

@FunctionHint(
  output = @DataTypeHint("ROW<"
    + "session_type STRING, "
    + "customer_name STRING, "
    + "order_count INT, "
    + "total_amount DECIMAL(10, 2), "
    + "session_start TIMESTAMP_LTZ(3), "
    + "session_duration BIGINT"
    + ">")
)
public class OrderSessionProcessor extends ProcessTableFunction<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderSessionProcessor.class);

  private static final String SESSION_REMINDER_TIMER = "SESSION_REMINDER";
  private static final String SESSION_TIMEOUT_TIMER = "SESSION_TIMEOUT";

  private static final String SESSION_TYPE_ACTIVE = "ACTIVE";
  private static final String SESSION_TYPE_COMPLETED = "COMPLETED";

  public static class SessionState {
    public String customerName;
    public double totalAmount = 0.0;
    public int orderCount = 0;
    public Instant sessionStart;
    public Instant lastActivity;

    public void addOrder(String customer, double amount, Instant timestamp) {
      if (sessionStart == null) {
        sessionStart = timestamp;
      }
      customerName = customer;
      totalAmount += amount;
      orderCount++;
      lastActivity = timestamp;
    }

    public void reset() {
      customerName = null;
      totalAmount = 0.0;
      orderCount = 0;
      sessionStart = null;
      lastActivity = null;
    }

    public boolean hasData() {
      return sessionStart != null && orderCount > 0;
    }

    public long getSessionDuration(Instant current) {
      if (sessionStart == null) {
        return 0;
      }
      return current.toEpochMilli() - sessionStart.toEpochMilli();
    }
  }

  public void eval(
    Context ctx,
    @StateHint(ttl = "1 hour") SessionState session,
    @ArgumentHint(SET_SEMANTIC_TABLE) Row order,
    Integer sessionTimeoutSec,
    Integer reminderOffsetSec
  ) throws Exception {
    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    Instant eventTime = timeCtx.time();
    String customerName = order.getFieldAs("customer_name");
    BigDecimal amount = order.getFieldAs("amount");

    session.addOrder(customerName, amount.doubleValue(), eventTime);

    Instant timeoutTime = eventTime.plus(Duration.ofSeconds(sessionTimeoutSec));
    Instant reminderTime = eventTime.plus(Duration.ofSeconds(reminderOffsetSec));

    timeCtx.registerOnTime(SESSION_TIMEOUT_TIMER, timeoutTime);
    timeCtx.registerOnTime(SESSION_REMINDER_TIMER, reminderTime);

    LOG.info(
      "[{}] Order added: ${} | Session total: ${} ({} orders) | Reminder: {}, Timeout: {}",
      customerName,
      String.format("%.2f", amount.doubleValue()),
      String.format("%.2f", session.totalAmount),
      session.orderCount,
      reminderTime,
      timeoutTime
    );
  }

  public void onTimer(OnTimerContext ctx, SessionState session) throws Exception {
    String timerName = ctx.currentTimer();
    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    Instant currentTime = timeCtx.time();

    if (SESSION_REMINDER_TIMER.equals(timerName)) {
      if (session.hasData()) {
        emitSessionResult(SESSION_TYPE_ACTIVE, session, currentTime);
        LOG.info(
          "[{}] Session reminder emitted: {} orders, ${}",
          session.customerName,
          session.orderCount,
          String.format("%.2f", session.totalAmount)
        );
      }
    } else if (SESSION_TIMEOUT_TIMER.equals(timerName)) {
      if (session.hasData()) {
        emitSessionResult(SESSION_TYPE_COMPLETED, session, currentTime);
        LOG.info(
          "[{}] Session completed: {} orders, ${}, duration: {}ms",
          session.customerName,
          session.orderCount,
          String.format("%.2f", session.totalAmount),
          session.getSessionDuration(currentTime)
        );
        session.reset();
      }
    }
  }

  private void emitSessionResult(
    String sessionType,
    SessionState session,
    Instant currentTime
  ) {
    Row result = Row.of(
      sessionType,
      session.customerName,
      session.orderCount,
      BigDecimal.valueOf(session.totalAmount),
      session.sessionStart,
      session.getSessionDuration(currentTime)
    );
    collect(result);
  }
}
