package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.WithinSumRangePredicate;

public class FilterTest {

  @Test
  public void testWithinSumRangePredicateIntColumn() throws DbException {
    // One data point should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("a", "b", "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    // The middle case
    testBase.put(0, 4);
    testBase.put(1, 1);
    testBase.put(2, 4);
    // Way out of range
    testBase.put(0, 4);
    testBase.put(1, 1);
    testBase.put(2, 10);
    // Right at the edge, but shouldn't be included
    testBase.put(0, 4);
    testBase.put(1, 1);
    testBase.put(2, 3);
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
    testBase.put(0, 4L);
    testBase.put(1, 1L);
    testBase.put(2, 4L);
    // Way out of range
    testBase.put(0, 4L);
    testBase.put(1, 1L);
    testBase.put(2, 10L);
    // Right at the edge, but shouldn't be included
    testBase.put(0, 4L);
    testBase.put(1, 1L);
    testBase.put(2, 3L);
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
    testBase.put(0, 4.0);
    testBase.put(1, 1.0);
    testBase.put(2, 4.0);
    // Way out of range
    testBase.put(0, 4.0);
    testBase.put(1, 1.0);
    testBase.put(2, 10.0);
    // Right at the edge, but shouldn't be in the tb after the filter
    testBase.put(0, 4.0);
    testBase.put(1, 1.0);
    testBase.put(2, 3.0);
    // Right inside
    testBase.put(0, 4.0);
    testBase.put(1, 1.0);
    testBase.put(2, 3.1);
    // Right outside
    testBase.put(0, 4.0);
    testBase.put(1, 1.0);
    testBase.put(2, 5.1);
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
    testBase.put(0, 4.0f);
    testBase.put(1, 1.0f);
    testBase.put(2, 4.0f);
    // Way out of range
    testBase.put(0, 4.0f);
    testBase.put(1, 1.0f);
    testBase.put(2, 10.0f);
    // Right at the edge, but shouldn't be in the tb after the filter
    testBase.put(0, 4.0f);
    testBase.put(1, 1.0f);
    testBase.put(2, 3.0f);
    // Right inside
    testBase.put(0, 4.0f);
    testBase.put(1, 1.0f);
    testBase.put(2, 3.1f);
    // Right outside
    testBase.put(0, 4.0f);
    testBase.put(1, 1.0f);
    testBase.put(2, 5.1f);
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
