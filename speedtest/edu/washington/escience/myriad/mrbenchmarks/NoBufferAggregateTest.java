package edu.washington.escience.myriad.mrbenchmarks;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.TupleSource;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.operator.agg.MultiGroupByAggregate;
import edu.washington.escience.myriad.operator.agg.SingleGroupByAggregateNoBuffer;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.util.TestUtils;

public class NoBufferAggregateTest {

  public TupleBatchBuffer generateRandomTuples(final int numTuples) {
    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testSingleGroupCount() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 0,
            new int[] { Aggregator.AGG_OP_COUNT });
    agg.open(null);
    TupleBatch tb = null;
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByCount(testBase, 0), actualResult);
  }

  @Test
  public void testSingleGroupAvg() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_AVG });
    agg.open(null);
    TupleBatch tb = null;
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByAvgLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupAvgNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_AVG });
    agg.open(null);
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByAvgLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupMax() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_MAX });
    agg.open(null);
    TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_MAX });
    agg.open(null);
    tb = null;
    result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 0, 1), actualResult);
  }

  @Test
  public void testSingleGroupMaxNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_MAX });
    agg.open(null);
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_MAX });
    agg.open(null);
    result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 0, 1), actualResult);
  }

  @Test
  public void testSingleGroupMin() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_MIN });
    agg.open(null);
    TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_MIN });
    agg.open(null);
    tb = null;
    result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 0, 1), actualResult);
  }

  @Test
  public void testSingleGroupMinNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_MIN });
    agg.open(null);
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_MIN });
    agg.open(null);
    result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 0, 1), actualResult);
  }

  @Test
  public void testSingleGroupSum() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_SUM });
    agg.open(null);
    TupleBatch tb = null;
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupBySumLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupSumNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 0 }, 1,
            new int[] { Aggregator.AGG_OP_SUM });
    agg.open(null);
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<SystemTestBase.Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupBySumLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testMultiGroupSum() throws DbException {
    final int numTuples = 1000000;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    long expectedFirst = 0;
    long expectedSecond = 0;
    // The idea of the tests is to generate altering data in the following scheme:
    // inserting { 0, 1, 2, i / 2 } on even rows, { 0, 1, 4, i / 2 } on odd rows
    for (long i = 0; i < numTuples; i++) {
      long value = i / 2;
      tbb.put(0, 0L);
      tbb.put(1, 1L);
      if (i % 2 == 0) {
        tbb.put(2, 2L);
        expectedSecond += value;
      } else {
        tbb.put(2, 4L);
      }
      tbb.put(3, value);
      expectedFirst += value;
    }

    // test for grouping at the first and second column
    // expected all the i / 2 to be sum up
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 3 }, new int[] { 0, 1 },
            new int[] { Aggregator.AGG_OP_SUM });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());
    assertEquals(expectedFirst, result.getLong(result.numColumns() - 1, 0));
    mga.close();

    // test for grouping at the first, second and third column
    // expecting half of i / 2 to be sum up on each group
    MultiGroupByAggregate mgaTwo =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 3 }, new int[] { 0, 1, 2 },
            new int[] { Aggregator.AGG_OP_SUM });
    mgaTwo.open(null);
    TupleBatch resultTwo = mgaTwo.nextReady();
    assertEquals(2, resultTwo.numTuples());
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 0));
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 1));
    mgaTwo.close();
  }

  @Test
  public void testSingleGroupStd() throws Exception {
    /* The source tuples -- integers 2 through 5 */
    int from = 2, to = 5;
    int n = to - from + 1; // we are using a biased version of variance
    final TupleBatchBuffer testBase =
        new TupleBatchBuffer(Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("group",
            "value")));
    int sum = 0;
    for (int i = from; i <= to; ++i) {
      testBase.put(0, Integer.valueOf(0));
      testBase.put(1, Integer.valueOf(i));
      sum += i;
    }

    /* Generate expected values for stdev */

    double mean = (double) sum / n;
    double diffSquared = 0.0;
    for (int i = from; i <= to; ++i) {
      double diff = i - mean;
      diffSquared += diff * diff;
    }
    double expectedStdev = Math.sqrt(diffSquared / n);

    /* Group by group, aggregate on value */
    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(new TupleSource(testBase), new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_STDEV });
    agg.open(null);
    TupleBatch tb = null;
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    assertEquals(expectedStdev, (double) result.get(1, 0), 0.000001);
  }
}
