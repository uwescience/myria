package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.WithinSumRangePredicate;

public class FilterTest {

  @Test
  public void testWithinSumRangePredicateIntColumn() throws DbException {
    // One data point should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("a", "b", "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    // The middle case
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 4);
    // Way out of range
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 10);
    // Right at the edge, but shouldn't be included
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 3);
    ImmutableList<Integer> operandList = ImmutableList.of(0, 1);
    Filter filter = new Filter(new WithinSumRangePredicate(2, operandList), new TupleSource(testBase));
    assertEquals(1, getRowCount(filter));
  }

  @Test
  public void testWithinSumRangePredicateLongColumn() throws DbException {
    // One data point should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("a", "b", "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    // The middle case
    testBase.putLong(0, 4L);
    testBase.putLong(1, 1L);
    testBase.putLong(2, 4L);
    // Way out of range
    testBase.putLong(0, 4L);
    testBase.putLong(1, 1L);
    testBase.putLong(2, 10L);
    // Right at the edge, but shouldn't be included
    testBase.putLong(0, 4L);
    testBase.putLong(1, 1L);
    testBase.putLong(2, 3L);
    ImmutableList<Integer> operandList = ImmutableList.of(0, 1);
    Filter filter = new Filter(new WithinSumRangePredicate(2, operandList), new TupleSource(testBase));
    assertEquals(1, getRowCount(filter));
  }

  @Test
  public void testWithinSumRangePredicateDoubleColumn() throws DbException {
    // Two data points should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.DOUBLE_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE), ImmutableList.of("a", "b",
            "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    // The middle case
    testBase.putDouble(0, 4.0);
    testBase.putDouble(1, 1.0);
    testBase.putDouble(2, 4.0);
    // Way out of range
    testBase.putDouble(0, 4.0);
    testBase.putDouble(1, 1.0);
    testBase.putDouble(2, 10.0);
    // Right at the edge, but shouldn't be in the tb after the filter
    testBase.putDouble(0, 4.0);
    testBase.putDouble(1, 1.0);
    testBase.putDouble(2, 3.0);
    // Right inside
    testBase.putDouble(0, 4.0);
    testBase.putDouble(1, 1.0);
    testBase.putDouble(2, 3.1);
    // Right outside
    testBase.putDouble(0, 4.0);
    testBase.putDouble(1, 1.0);
    testBase.putDouble(2, 5.1);
    ImmutableList<Integer> operandList = ImmutableList.of(0, 1);
    Filter filter = new Filter(new WithinSumRangePredicate(2, operandList), new TupleSource(testBase));
    assertEquals(2, getRowCount(filter));
  }

  @Test
  public void testWithinSumRangePredicateFloatColumn() throws DbException {
    // Two data points should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.FLOAT_TYPE, Type.FLOAT_TYPE, Type.FLOAT_TYPE), ImmutableList.of("a", "b", "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    // The middle case
    testBase.putFloat(0, 4.0f);
    testBase.putFloat(1, 1.0f);
    testBase.putFloat(2, 4.0f);
    // Way out of range
    testBase.putFloat(0, 4.0f);
    testBase.putFloat(1, 1.0f);
    testBase.putFloat(2, 10.0f);
    // Right at the edge, but shouldn't be in the tb after the filter
    testBase.putFloat(0, 4.0f);
    testBase.putFloat(1, 1.0f);
    testBase.putFloat(2, 3.0f);
    // Right inside
    testBase.putFloat(0, 4.0f);
    testBase.putFloat(1, 1.0f);
    testBase.putFloat(2, 3.1f);
    // Right outside
    testBase.putFloat(0, 4.0f);
    testBase.putFloat(1, 1.0f);
    testBase.putFloat(2, 5.1f);
    ImmutableList<Integer> operandList = ImmutableList.of(0, 1);
    Filter filter = new Filter(new WithinSumRangePredicate(2, operandList), new TupleSource(testBase));
    assertEquals(2, getRowCount(filter));
  }

  /*
   * helper method for getting the row count
   */
  private static int getRowCount(Operator operator) throws DbException {
    operator.open(null);
    int count = 0;
    TupleBatch tb = null;
    while (!operator.eos()) {
      tb = operator.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    return count;
  }
}
