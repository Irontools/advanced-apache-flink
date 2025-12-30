package dev.irontools.flink;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<tier_name STRING, tier_min DOUBLE, tier_max DOUBLE, is_in_tier BOOLEAN>"))
public class CategoryTierFunction extends TableFunction<Row> {
  private static final String BUDGET_TIER = "Budget";
  private static final double BUDGET_MIN = 0.0;
  private static final double BUDGET_MAX = 300.0;

  private static final String MIDRANGE_TIER = "Mid-range";
  private static final double MIDRANGE_MIN = 300.0;
  private static final double MIDRANGE_MAX = 800.0;

  private static final String PREMIUM_TIER = "Premium";
  private static final double PREMIUM_MIN = 800.0;
  private static final double PREMIUM_MAX = 9999.0;

  public void eval(Double amount) {
    boolean isInBudget = amount >= BUDGET_MIN && amount < BUDGET_MAX;
    boolean isInMidrange = amount >= MIDRANGE_MIN && amount < MIDRANGE_MAX;
    boolean isInPremium = amount >= PREMIUM_MIN;

    collect(Row.of(BUDGET_TIER, BUDGET_MIN, BUDGET_MAX, isInBudget));
    collect(Row.of(MIDRANGE_TIER, MIDRANGE_MIN, MIDRANGE_MAX, isInMidrange));
    collect(Row.of(PREMIUM_TIER, PREMIUM_MIN, PREMIUM_MAX, isInPremium));
  }
}
