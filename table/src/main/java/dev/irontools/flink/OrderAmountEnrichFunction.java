package dev.irontools.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<tier STRING, amount_difference DOUBLE, tier_index INT>"))
public class OrderAmountEnrichFunction extends TableFunction<Row> {
  private static final String BUDGET_TIER = "Budget";
  private static final double BUDGET_MIN = 0.0;
  private static final double BUDGET_MAX = 300.0;
  private static final int BUDGET_INDEX = 0;

  private static final String MIDRANGE_TIER = "Mid-range";
  private static final double MIDRANGE_MIN = 300.0;
  private static final double MIDRANGE_MAX = 800.0;
  private static final int MIDRANGE_INDEX = 1;

  private static final String PREMIUM_TIER = "Premium";
  private static final double PREMIUM_MIN = 800.0;
  private static final int PREMIUM_INDEX = 2;

  public void eval(Double amount) {
    if (amount < BUDGET_MAX) {
      collect(Row.of(BUDGET_TIER, amount - BUDGET_MIN, BUDGET_INDEX));
    } else if (amount < MIDRANGE_MAX) {
      collect(Row.of(MIDRANGE_TIER, amount - MIDRANGE_MIN, MIDRANGE_INDEX));
    } else {
      collect(Row.of(PREMIUM_TIER, amount - PREMIUM_MIN, PREMIUM_INDEX));
    }
  }
}
