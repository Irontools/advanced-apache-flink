package dev.irontools.flink;

import net.openhft.hashing.LongTupleHashFunction;
import org.apache.flink.table.functions.ScalarFunction;

public class XxHashFunction extends ScalarFunction {
  public String eval(String string) {
    long[] hash = LongTupleHashFunction.xx128().hashChars(string);
    return Long.toHexString(hash[0]) + Long.toHexString(hash[1]);
  }
}
