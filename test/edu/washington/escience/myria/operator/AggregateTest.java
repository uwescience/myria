package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.column.builder.DoubleColumnBuilder;
import edu.washington.escience.myria.column.builder.FloatColumnBuilder;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.util.TestEnvVars;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class AggregateTest {

  /**
   * Ensure that the given Schema matches the expected numeric aggregate types for the given Type.
   * 
   * All numeric aggs, in order: COUNT, MIN, MAX, SUM, AVG, STDEV
   * 
   * MIN,MAX match the input type
   * 
   * SUM is the big form (int->long) and (float->double) of the input type
   * 
   * COUNT is always long
   * 
   * AVG and STDEV are always double
   * 
   * @param schema the schema
   * @param type the type being aggregated
   */
  private void allNumericAggsTestSchema(final Schema schema, final Type type) {
    Type bigType = type;
    if (type == Type.INT_TYPE) {
      bigType = Type.LONG_TYPE;
    } else if (type == Type.FLOAT_TYPE) {
      bigType = Type.DOUBLE_TYPE;
    }
    assertEquals(6, schema.numColumns());
    assertEquals(Type.LONG_TYPE, schema.getColumnType(0));
    assertEquals(type, schema.getColumnType(1));
    assertEquals(type, schema.getColumnType(2));
    assertEquals(bigType, schema.getColumnType(3));
    assertEquals(Type.DOUBLE_TYPE, schema.getColumnType(4));
    assertEquals(Type.DOUBLE_TYPE, schema.getColumnType(5));
  }

  /**
   * For ensure that the given Schema matches the expected non-numeric aggregate types for the given Type.
   * 
   * All non-numeric aggs, in order: COUNT, MIN, MAX
   * 
   * MIN,MAX match the input type
   * 
   * COUNT is always long
   * 
   * @param schema the schema
   * @param type the type being aggregated
   */
  private void allNonNumericAggsTestSchema(final Schema schema, final Type type) {
    assertEquals(3, schema.numColumns());
    assertEquals(Type.LONG_TYPE, schema.getColumnType(0));
    assertEquals(type, schema.getColumnType(1));
    assertEquals(type, schema.getColumnType(2));
  }

  /**
   * Helper function to turn a single {@link ColumnBuilder} into a {@link TupleBatch}.
   * 
   * @param builder the column builder
   * @return the TupleBatch
   */
  private TupleBatch makeTrivialTupleBatch(ColumnBuilder<?> builder) {
    Schema schema = Schema.of(ImmutableList.of(builder.getType()), ImmutableList.of("col0"));
    return new TupleBatch(schema, ImmutableList.<Column<?>> of(builder.build()));
  }

  /**
   * Helper function to instantiate an aggregator and do the aggregation. Do not use if more than one TupleBatch are
   * expected.
   * 
   * @param builder the tuples to be aggregated
   * @param aggOps the aggregate operations over the column
   * @return a single TupleBatch containing the results of the aggregation
   * @throws Exception if there is an error
   */
  private TupleBatch doAggOpsToCol(ColumnBuilder<?> builder, int[] aggOps) throws Exception {
    TupleSource source = new TupleSource(makeTrivialTupleBatch(builder));
    int[] fields = new int[aggOps.length];
    for (int i = 0; i < fields.length; ++i) {
      fields[i] = 0;
    }
    Aggregate agg = new Aggregate(source, fields, aggOps);
    /* Do it -- this should cause an error. */
    agg.open(TestEnvVars.get());
    TupleBatch tb = agg.nextReady();
    agg.close();
    return tb;
  }

  @Test
  public void testNumericAggs() throws Exception {
    ColumnBuilder<?> builder;
    TupleBatch tb;
    int[] numericAggBitsInOrder =
        new int[] {
            Aggregator.AGG_OP_COUNT, Aggregator.AGG_OP_MIN, Aggregator.AGG_OP_MAX, Aggregator.AGG_OP_SUM,
            Aggregator.AGG_OP_AVG, Aggregator.AGG_OP_STDEV };
    int[] justCount = new int[] { Aggregator.AGG_OP_COUNT };
    int[] justMin = new int[] { Aggregator.AGG_OP_MIN };
    int[] justMax = new int[] { Aggregator.AGG_OP_MAX };
    int[] justSum = new int[] { Aggregator.AGG_OP_SUM };
    int[] justAvg = new int[] { Aggregator.AGG_OP_AVG };
    int[] justStdev = new int[] { Aggregator.AGG_OP_STDEV };

    /* Ints, all as a group */
    int[] ints = new int[] { 3, 5, 6 };
    builder = new IntColumnBuilder();
    for (int i : ints) {
      builder.appendInt(i);
    }
    tb = doAggOpsToCol(builder, numericAggBitsInOrder);
    allNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(ints.length, tb.getLong(0, 0));
    assertEquals(Ints.min(ints), tb.getInt(1, 0));
    assertEquals(Ints.max(ints), tb.getInt(2, 0));
    assertEquals(3 + 5 + 6, tb.getLong(3, 0));
    assertEquals((3 + 5 + 6) / 3.0, tb.getDouble(4, 0), 0.0001);
    // Wolfram Alpha: population standard deviation {3,5,6}
    assertEquals(1.2472, tb.getDouble(5, 0), 0.0001);
    /* Ints, one aggregate at a time */
    tb = doAggOpsToCol(builder, justCount);
    assertEquals(ints.length, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justMin);
    assertEquals(Ints.min(ints), tb.getInt(0, 0));
    tb = doAggOpsToCol(builder, justMax);
    assertEquals(Ints.max(ints), tb.getInt(0, 0));
    tb = doAggOpsToCol(builder, justSum);
    assertEquals(3 + 5 + 6, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justAvg);
    assertEquals((3 + 5 + 6) / 3.0, tb.getDouble(0, 0), 0.0001);
    tb = doAggOpsToCol(builder, justStdev);
    assertEquals(1.2472, tb.getDouble(0, 0), 0.0001);

    /* Longs */
    long[] longs = new long[] { 3, 5, 9 };
    builder = new LongColumnBuilder();
    for (long l : longs) {
      builder.appendLong(l);
    }
    tb = doAggOpsToCol(builder, numericAggBitsInOrder);
    allNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(longs.length, tb.getLong(0, 0));
    assertEquals(Longs.min(longs), tb.getLong(1, 0));
    assertEquals(Longs.max(longs), tb.getLong(2, 0));
    assertEquals(3 + 5 + 9, tb.getLong(3, 0));
    assertEquals((3 + 5 + 9) / 3.0, tb.getDouble(4, 0), 0.0001);
    // Wolfram Alpha: population standard deviation {3,5,9}
    assertEquals(2.4944, tb.getDouble(5, 0), 0.0001);
    /* Longs, one aggregate at a time */
    tb = doAggOpsToCol(builder, justCount);
    assertEquals(longs.length, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justMin);
    assertEquals(Longs.min(longs), tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justMax);
    assertEquals(Longs.max(longs), tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justSum);
    assertEquals(3 + 5 + 9, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justAvg);
    assertEquals((3 + 5 + 9) / 3.0, tb.getDouble(0, 0), 0.0001);
    tb = doAggOpsToCol(builder, justStdev);
    assertEquals(2.4944, tb.getDouble(0, 0), 0.0001);

    /* Floats */
    float[] floats = new float[] { 3, 5, 11 };
    builder = new FloatColumnBuilder();
    for (float f : floats) {
      builder.appendFloat(f);
    }
    tb = doAggOpsToCol(builder, numericAggBitsInOrder);
    allNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(floats.length, tb.getLong(0, 0));
    assertEquals(Floats.min(floats), tb.getFloat(1, 0), 0.000001);
    assertEquals(Floats.max(floats), tb.getFloat(2, 0), 0.000001);
    assertEquals(3f + 5f + 11f, tb.getDouble(3, 0), 0.0000001);
    assertEquals((3 + 5 + 11) / 3.0, tb.getDouble(4, 0), 0.0001);
    // Wolfram Alpha: population standard deviation {3,5,11}
    assertEquals(3.3993, tb.getDouble(5, 0), 0.0001);
    /* Floats, one aggregate at a time */
    tb = doAggOpsToCol(builder, justCount);
    assertEquals(floats.length, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justMin);
    assertEquals(Floats.min(floats), tb.getFloat(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justMax);
    assertEquals(Floats.max(floats), tb.getFloat(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justSum);
    assertEquals(3f + 5f + 11f, tb.getDouble(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justAvg);
    assertEquals((3f + 5f + 11f) / 3.0, tb.getDouble(0, 0), 0.0001);
    tb = doAggOpsToCol(builder, justStdev);
    assertEquals(3.3993, tb.getDouble(0, 0), 0.0001);

    /* Double */
    double[] doubles = new double[] { 3, 5, 13 };
    builder = new DoubleColumnBuilder();
    for (double d : doubles) {
      builder.appendDouble(d);
    }
    tb = doAggOpsToCol(builder, numericAggBitsInOrder);
    allNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(doubles.length, tb.getLong(0, 0));
    assertEquals(Doubles.min(doubles), tb.getDouble(1, 0), 0.000001);
    assertEquals(Doubles.max(doubles), tb.getDouble(2, 0), 0.000001);
    assertEquals(3 + 5 + 13, tb.getDouble(3, 0), 0.0000001);
    assertEquals((3 + 5 + 13) / 3.0, tb.getDouble(4, 0), 0.0001);
    // Wolfram Alpha: population standard deviation {3,5,13}
    assertEquals(4.3205, tb.getDouble(5, 0), 0.0001);
    /* Doubles, one aggregate at a time */
    tb = doAggOpsToCol(builder, justCount);
    assertEquals(doubles.length, tb.getLong(0, 0));
    tb = doAggOpsToCol(builder, justMin);
    assertEquals(Doubles.min(doubles), tb.getDouble(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justMax);
    assertEquals(Doubles.max(doubles), tb.getDouble(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justSum);
    assertEquals(3f + 5f + 13f, tb.getDouble(0, 0), 0.000001);
    tb = doAggOpsToCol(builder, justAvg);
    assertEquals((3f + 5f + 13f) / 3.0, tb.getDouble(0, 0), 0.0001);
    tb = doAggOpsToCol(builder, justStdev);
    assertEquals(4.3205, tb.getDouble(0, 0), 0.0001);
  }

  @Test
  public void testNonNumericAggs() throws Exception {
    ColumnBuilder<?> builder;
    TupleBatch tb;
    int[] nonNumAggBitsInOrder = new int[] { Aggregator.AGG_OP_COUNT, Aggregator.AGG_OP_MIN, Aggregator.AGG_OP_MAX, };

    /* Dates */
    DateTime[] dates =
        new DateTime[] {
            DateTime.parse("2014-04-01T11:30"), DateTime.parse("2014-04-01T11:31"), DateTime.parse("2012-02-29T12:00") };
    builder = new DateTimeColumnBuilder();
    for (DateTime d : dates) {
      builder.appendDateTime(d);
    }
    tb = doAggOpsToCol(builder, nonNumAggBitsInOrder);
    allNonNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(dates.length, tb.getLong(0, 0));
    assertEquals(DateTime.parse("2012-02-29T12:00"), tb.getDateTime(1, 0));
    assertEquals(DateTime.parse("2014-04-01T11:31"), tb.getDateTime(2, 0));

    /* Strings */
    String[] strings = new String[] { "abcd", "abc", "abcde", "fghij0", "fghij1" };
    builder = new StringColumnBuilder();
    for (String s : strings) {
      builder.appendString(s);
    }
    tb = doAggOpsToCol(builder, nonNumAggBitsInOrder);
    allNonNumericAggsTestSchema(tb.getSchema(), builder.getType());
    assertEquals(strings.length, tb.getLong(0, 0));
    assertEquals("abc", tb.getString(1, 0));
    assertEquals("fghij1", tb.getString(2, 0));
  }

  public TupleBatchBuffer generateRandomTuples(final int numTuples) {
    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testSingleGroupAvg() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_AVG });
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
    final HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByAvgLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupAvgNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_AVG });
    agg.open(null);
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByAvgLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupMax() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MAX });
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
    HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MAX });
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
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MAX });
    agg.open(null);
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMax(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MAX });
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
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MIN });
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
    HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MIN });
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
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MIN });
    agg.open(null);
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupByMin(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MIN });
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
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_SUM });
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
    final HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupBySumLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupSumNonBlocking() throws DbException, InterruptedException {
    final int maxValue = 200000;
    final int numTuples = (int) (Math.random() * maxValue);

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_SUM });
    agg.open(null);
    final TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while (!agg.eos()) {
      TupleBatch tb = agg.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }
    agg.close();
    final HashMap<Tuple, Integer> actualResult = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(TestUtils.groupBySumLongColumn(testBase, 1, 0), actualResult);
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
      testBase.putInt(0, 0);
      testBase.putInt(1, i);
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
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_STDEV });
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
    tb = result.popAny();
    assertEquals(expectedStdev, tb.getDouble(1, 0), 0.000001);
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
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
        expectedSecond += value;
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, value);
      expectedFirst += value;
    }

    // test for grouping at the first and second column
    // expected all the i / 2 to be sum up
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 3 },
            new int[] { Aggregator.AGG_OP_SUM });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());
    assertEquals(expectedFirst, result.getLong(result.numColumns() - 1, 0));
    mga.close();

    // test for grouping at the first, second and third column
    // expecting half of i / 2 to be sum up on each group
    MultiGroupByAggregate mgaTwo =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1, 2 }, new int[] { 3 },
            new int[] { Aggregator.AGG_OP_SUM });
    mgaTwo.open(null);
    TupleBatch resultTwo = mgaTwo.nextReady();
    assertEquals(2, resultTwo.numTuples());
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 0));
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 1));
    mgaTwo.close();
  }

  @Test
  public void testMultiGroupAvg() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    double expected = 0.0;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
        expected += i;
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, i / 2);
    }
    expected /= numTuples;
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1, 2 }, new int[] { 3 },
            new int[] { Aggregator.AGG_OP_AVG });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(2, result.numTuples());
    assertEquals(expected, result.getDouble(result.numColumns() - 1, 0), 0.000001);
    mga.close();
  }

  @Test
  public void testMultiGroupMin() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    long expected = 0;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, i / 2);
    }
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 3 },
            new int[] { Aggregator.AGG_OP_MIN });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());
    assertEquals(expected, result.getLong(result.numColumns() - 1, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupMax() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    long expected = numTuples - 1;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, i);
    }
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 3 },
            new int[] { Aggregator.AGG_OP_MAX });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());
    assertEquals(expected, result.getLong(result.numColumns() - 1, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupMaxMultiColumn() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);

    long expectedMin = 0;
    long expectedMax = numTuples - 1 + expectedMin;
    for (long i = expectedMin; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, i);
    }
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 3, 3 }, new int[] {
            Aggregator.AGG_OP_MAX, Aggregator.AGG_OP_MIN });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());

    assertEquals(4, result.getSchema().numColumns());
    assertEquals(expectedMin, result.getLong(result.numColumns() - 1, 0));
    assertEquals(expectedMax, result.getLong(result.numColumns() - 2, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupCountMultiColumn() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, 1L);
      if (i % 2 == 0) {
        tbb.putLong(2, 2L);
      } else {
        tbb.putLong(2, 4L);
      }
      tbb.putLong(3, i);
    }
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 0 },
            new int[] { Aggregator.AGG_OP_COUNT });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertEquals(1, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    assertEquals(numTuples, result.getLong(result.numColumns() - 1, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupCountMultiColumnEmpty() throws DbException {
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    MultiGroupByAggregate mga =
        new MultiGroupByAggregate(new TupleSource(tbb), new int[] { 0, 1 }, new int[] { 0 },
            new int[] { Aggregator.AGG_OP_COUNT });
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNull(result);
    mga.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testLongAggOverflow() throws Exception {
    LongColumnBuilder builder = new LongColumnBuilder().appendLong(Long.MAX_VALUE - 1).appendLong(3);
    doAggOpsToCol(builder, new int[] { Aggregator.AGG_OP_SUM });
  }

  @Test(expected = ArithmeticException.class)
  public void testLongAggUnderflow() throws Exception {
    LongColumnBuilder builder = new LongColumnBuilder().appendLong(Long.MIN_VALUE + 1).appendLong(-3);
    doAggOpsToCol(builder, new int[] { Aggregator.AGG_OP_SUM });
  }
}
