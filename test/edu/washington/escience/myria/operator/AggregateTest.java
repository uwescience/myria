package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.BooleanColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.column.builder.DoubleColumnBuilder;
import edu.washington.escience.myria.column.builder.FloatColumnBuilder;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregatorFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.util.HashUtils;
import edu.washington.escience.myria.util.TestEnvVars;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class AggregateTest {

  /**
   * Ensure that the given Schema matches the expected numeric aggregate types for the given Type. All numeric aggs, in
   * order: COUNT, MIN, MAX, SUM, AVG, STDEV MIN,MAX match the input type SUM is the big form (int->long) and
   * (float->double) of the input type COUNT is always long AVG and STDEV are always double
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
   * For ensure that the given Schema matches the expected non-numeric aggregate types for the given Type. All
   * non-numeric aggs, in order: COUNT, MIN, MAX MIN,MAX match the input type COUNT is always long
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
  private TupleBatch makeTrivialTupleBatch(final ColumnBuilder<?> builder) {
    Schema schema = Schema.of(ImmutableList.of(builder.getType()), ImmutableList.of("col0"));
    return new TupleBatch(schema, ImmutableList.<Column<?>>of(builder.build()));
  }

  /**
   * Helper function to instantiate an aggregator and do the aggregation. Do not use if more than one TupleBatch are
   * expected.
   *
   * @param builder the tuples to be aggregated
   * @param aggOps the aggregate operations over the column
   * @param noColumns whether to group by no columns (if true) or to append a constant value single column and group by
   *        it (if false).
   * @return a single TupleBatch containing the results of the aggregation
   * @throws Exception if there is an error
   */
  private TupleBatch doAggOpsToCol(
      final ColumnBuilder<?> builder, final AggregationOp[] aggOps, final boolean noColumns)
      throws Exception {
    if (noColumns == false) {
      return doAggOpsToSingleCol(builder, aggOps);
    }
    BatchTupleSource source = new BatchTupleSource(makeTrivialTupleBatch(builder));
    AggregatorFactory aggs = new PrimitiveAggregatorFactory(0, aggOps);
    Aggregate agg = new Aggregate(source, new int[] {}, aggs);
    /* Do it -- this should cause an error. */
    agg.open(TestEnvVars.get());
    TupleBatch tb = agg.nextReady();
    agg.close();
    return tb;
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
  private TupleBatch doAggOpsToSingleCol(
      final ColumnBuilder<?> builder, final AggregationOp[] aggOps) throws Exception {
    TupleBatch trivialTb = makeTrivialTupleBatch(builder);
    ConstantValueColumn constCol = new ConstantValueColumn(3, Type.INT_TYPE, trivialTb.numTuples());
    Schema newSchema =
        Schema.merge(trivialTb.getSchema(), Schema.ofFields("_const_col", Type.INT_TYPE));
    List<Column<?>> columns =
        ImmutableList.<Column<?>>builder().addAll(trivialTb.getDataColumns()).add(constCol).build();
    BatchTupleSource source = new BatchTupleSource(new TupleBatch(newSchema, columns));
    AggregatorFactory[] aggs = new AggregatorFactory[aggOps.length];
    for (int i = 0; i < aggs.length; ++i) {
      aggs[i] = new PrimitiveAggregatorFactory(0, aggOps[i]);
    }
    Aggregate agg = new Aggregate(source, new int[] {trivialTb.numColumns()}, aggs);
    /* Do it -- this should cause an error. */
    agg.open(TestEnvVars.get());
    TupleBatch tb = agg.nextReady();
    agg.close();
    /* Take the 1st through nth column, because the first column is the thing we grouped by. */
    int[] colsToKeep = new int[tb.numColumns() - 1];
    for (int i = 0; i < colsToKeep.length; ++i) {
      colsToKeep[i] = i + 1;
    }
    return tb.selectColumns(colsToKeep);
  }

  @Test
  public void testNumericAggs() throws Exception {
    ColumnBuilder<?> builder;
    TupleBatch tb;
    AggregationOp[] numericAggBitsInOrder =
        new AggregationOp[] {
          AggregationOp.COUNT,
          AggregationOp.MIN,
          AggregationOp.MAX,
          AggregationOp.SUM,
          AggregationOp.AVG,
          AggregationOp.STDEV
        };
    AggregationOp[] justCount = new AggregationOp[] {AggregationOp.COUNT};
    AggregationOp[] justMin = new AggregationOp[] {AggregationOp.MIN};
    AggregationOp[] justMax = new AggregationOp[] {AggregationOp.MAX};
    AggregationOp[] justSum = new AggregationOp[] {AggregationOp.SUM};
    AggregationOp[] justAvg = new AggregationOp[] {AggregationOp.AVG};
    AggregationOp[] justStdev = new AggregationOp[] {AggregationOp.STDEV};

    for (int variant = 0; variant < 2; ++variant) {
      /* Whether to group by zero or 1 columns. */
      boolean noColumns = (variant == 0);

      /* Ints, all as a group */
      int[] ints = new int[] {3, 5, 6};
      builder = new IntColumnBuilder();
      for (int i : ints) {
        builder.appendInt(i);
      }
      tb = doAggOpsToCol(builder, numericAggBitsInOrder, noColumns);
      allNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(ints.length, tb.getLong(0, 0));
      assertEquals(Ints.min(ints), tb.getInt(1, 0));
      assertEquals(Ints.max(ints), tb.getInt(2, 0));
      assertEquals(3 + 5 + 6, tb.getLong(3, 0));
      assertEquals((3 + 5 + 6) / 3.0, tb.getDouble(4, 0), 0.0001);
      // Wolfram Alpha: population standard deviation {3,5,6}
      assertEquals(1.2472, tb.getDouble(5, 0), 0.0001);
      /* Ints, one aggregate at a time */
      tb = doAggOpsToCol(builder, justCount, noColumns);
      assertEquals(ints.length, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justMin, noColumns);
      assertEquals(Ints.min(ints), tb.getInt(0, 0));
      tb = doAggOpsToCol(builder, justMax, noColumns);
      assertEquals(Ints.max(ints), tb.getInt(0, 0));
      tb = doAggOpsToCol(builder, justSum, noColumns);
      assertEquals(3 + 5 + 6, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justAvg, noColumns);
      assertEquals((3 + 5 + 6) / 3.0, tb.getDouble(0, 0), 0.0001);
      tb = doAggOpsToCol(builder, justStdev, noColumns);
      assertEquals(1.2472, tb.getDouble(0, 0), 0.0001);

      /* Longs */
      long[] longs = new long[] {3, 5, 9};
      builder = new LongColumnBuilder();
      for (long l : longs) {
        builder.appendLong(l);
      }
      tb = doAggOpsToCol(builder, numericAggBitsInOrder, noColumns);
      allNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(longs.length, tb.getLong(0, 0));
      assertEquals(Longs.min(longs), tb.getLong(1, 0));
      assertEquals(Longs.max(longs), tb.getLong(2, 0));
      assertEquals(3 + 5 + 9, tb.getLong(3, 0));
      assertEquals((3 + 5 + 9) / 3.0, tb.getDouble(4, 0), 0.0001);
      // Wolfram Alpha: population standard deviation {3,5,9}
      assertEquals(2.4944, tb.getDouble(5, 0), 0.0001);
      /* Longs, one aggregate at a time */
      tb = doAggOpsToCol(builder, justCount, noColumns);
      assertEquals(longs.length, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justMin, noColumns);
      assertEquals(Longs.min(longs), tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justMax, noColumns);
      assertEquals(Longs.max(longs), tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justSum, noColumns);
      assertEquals(3 + 5 + 9, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justAvg, noColumns);
      assertEquals((3 + 5 + 9) / 3.0, tb.getDouble(0, 0), 0.0001);
      tb = doAggOpsToCol(builder, justStdev, noColumns);
      assertEquals(2.4944, tb.getDouble(0, 0), 0.0001);

      /* Floats */
      float[] floats = new float[] {3, 5, 11};
      builder = new FloatColumnBuilder();
      for (float f : floats) {
        builder.appendFloat(f);
      }
      tb = doAggOpsToCol(builder, numericAggBitsInOrder, noColumns);
      allNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(floats.length, tb.getLong(0, 0));
      assertEquals(Floats.min(floats), tb.getFloat(1, 0), 0.000001);
      assertEquals(Floats.max(floats), tb.getFloat(2, 0), 0.000001);
      assertEquals(3f + 5f + 11f, tb.getDouble(3, 0), 0.0000001);
      assertEquals((3 + 5 + 11) / 3.0, tb.getDouble(4, 0), 0.0001);
      // Wolfram Alpha: population standard deviation {3,5,11}
      assertEquals(3.3993, tb.getDouble(5, 0), 0.0001);
      /* Floats, one aggregate at a time */
      tb = doAggOpsToCol(builder, justCount, noColumns);
      assertEquals(floats.length, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justMin, noColumns);
      assertEquals(Floats.min(floats), tb.getFloat(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justMax, noColumns);
      assertEquals(Floats.max(floats), tb.getFloat(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justSum, noColumns);
      assertEquals(3f + 5f + 11f, tb.getDouble(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justAvg, noColumns);
      assertEquals((3f + 5f + 11f) / 3.0, tb.getDouble(0, 0), 0.0001);
      tb = doAggOpsToCol(builder, justStdev, noColumns);
      assertEquals(3.3993, tb.getDouble(0, 0), 0.0001);

      /* Double */
      double[] doubles = new double[] {3, 5, 13};
      builder = new DoubleColumnBuilder();
      for (double d : doubles) {
        builder.appendDouble(d);
      }
      tb = doAggOpsToCol(builder, numericAggBitsInOrder, noColumns);
      allNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(doubles.length, tb.getLong(0, 0));
      assertEquals(Doubles.min(doubles), tb.getDouble(1, 0), 0.000001);
      assertEquals(Doubles.max(doubles), tb.getDouble(2, 0), 0.000001);
      assertEquals(3 + 5 + 13, tb.getDouble(3, 0), 0.0000001);
      assertEquals((3 + 5 + 13) / 3.0, tb.getDouble(4, 0), 0.0001);
      // Wolfram Alpha: population standard deviation {3,5,13}
      assertEquals(4.3205, tb.getDouble(5, 0), 0.0001);
      /* Doubles, one aggregate at a time */
      tb = doAggOpsToCol(builder, justCount, noColumns);
      assertEquals(doubles.length, tb.getLong(0, 0));
      tb = doAggOpsToCol(builder, justMin, noColumns);
      assertEquals(Doubles.min(doubles), tb.getDouble(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justMax, noColumns);
      assertEquals(Doubles.max(doubles), tb.getDouble(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justSum, noColumns);
      assertEquals(3f + 5f + 13f, tb.getDouble(0, 0), 0.000001);
      tb = doAggOpsToCol(builder, justAvg, noColumns);
      assertEquals((3f + 5f + 13f) / 3.0, tb.getDouble(0, 0), 0.0001);
      tb = doAggOpsToCol(builder, justStdev, noColumns);
      assertEquals(4.3205, tb.getDouble(0, 0), 0.0001);
    }
  }

  @Test
  public void testNonNumericAggs() throws Exception {
    ColumnBuilder<?> builder;
    TupleBatch tb;
    AggregationOp[] nonNumAggBitsInOrder =
        new AggregationOp[] {AggregationOp.COUNT, AggregationOp.MIN, AggregationOp.MAX};

    for (int variant = 0; variant < 2; ++variant) {
      boolean noColumns = (variant == 0);
      /* Dates */
      DateTime[] dates =
          new DateTime[] {
            DateTime.parse("2014-04-01T11:30"),
            DateTime.parse("2014-04-01T11:31"),
            DateTime.parse("2012-02-29T12:00")
          };
      builder = new DateTimeColumnBuilder();
      for (DateTime d : dates) {
        builder.appendDateTime(d);
      }
      tb = doAggOpsToCol(builder, nonNumAggBitsInOrder, noColumns);
      allNonNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(dates.length, tb.getLong(0, 0));
      assertEquals(DateTime.parse("2012-02-29T12:00"), tb.getDateTime(1, 0));
      assertEquals(DateTime.parse("2014-04-01T11:31"), tb.getDateTime(2, 0));

      /* Strings */
      String[] strings = new String[] {"abcd", "abc", "abcde", "fghij0", "fghij1"};
      builder = new StringColumnBuilder();
      for (String s : strings) {
        builder.appendString(s);
      }
      tb = doAggOpsToCol(builder, nonNumAggBitsInOrder, noColumns);
      allNonNumericAggsTestSchema(tb.getSchema(), builder.getType());
      assertEquals(strings.length, tb.getLong(0, 0));
      assertEquals("abc", tb.getString(1, 0));
      assertEquals("fghij1", tb.getString(2, 0));

      /* Booleans */
      AggregationOp[] booleanAggs = new AggregationOp[] {AggregationOp.COUNT};
      boolean[] booleans = new boolean[] {true, false, true};
      builder = new BooleanColumnBuilder();
      for (boolean b : booleans) {
        builder.appendBoolean(b);
      }
      tb = doAggOpsToCol(builder, booleanAggs, noColumns);
      assertEquals(1, tb.getSchema().numColumns());
      assertEquals(Type.LONG_TYPE, tb.getSchema().getColumnType(0));
      assertEquals(booleans.length, tb.getLong(0, 0));
    }
  }

  public TupleBatchBuffer generateRandomTuples(final int numTuples) {
    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testSingleGroupAvg() throws DbException, InterruptedException {
    final int numTuples = 2 * TupleBatch.BATCH_SIZE + 1;

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final Aggregate agg =
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {1},
            new PrimitiveAggregatorFactory(0, AggregationOp.AVG));
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
  public void testSingleGroupMax() throws DbException, InterruptedException {
    final int numTuples = 2 * TupleBatch.BATCH_SIZE + 1;

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    Aggregate agg =
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {1},
            new PrimitiveAggregatorFactory(0, AggregationOp.MAX));
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
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.MAX));
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
  public void testSingleGroupMin() throws DbException, InterruptedException {
    final int numTuples = 2 * TupleBatch.BATCH_SIZE + 1;

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    Aggregate agg =
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {1},
            new PrimitiveAggregatorFactory(0, AggregationOp.MIN));
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
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.MIN));
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
  public void testSingleGroupSum() throws DbException, InterruptedException {
    final int numTuples = 2 * TupleBatch.BATCH_SIZE + 1;

    final TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    final Aggregate agg =
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {1},
            new PrimitiveAggregatorFactory(0, AggregationOp.SUM));
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
  public void testSingleGroupStd() throws Exception {
    /* The source tuples -- integers 2 through 5 */
    int from = 2, to = 5;
    int n = to - from + 1; // we are using a biased version of variance
    final TupleBatchBuffer testBase =
        new TupleBatchBuffer(
            Schema.of(
                ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE),
                ImmutableList.of("group", "value")));
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
    final Aggregate agg =
        new Aggregate(
            new BatchTupleSource(testBase),
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.STDEV));
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
    final int numTuples = 2 * TupleBatch.BATCH_SIZE + 2;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(3, AggregationOp.SUM));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(expectedFirst, result.getLong(result.numColumns() - 1, 0));
    mga.close();

    // test for grouping at the first, second and third column
    // expecting half of i / 2 to be sum up on each group
    Aggregate mgaTwo =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1, 2},
            new PrimitiveAggregatorFactory(3, AggregationOp.SUM));
    mgaTwo.open(null);
    TupleBatch resultTwo = mgaTwo.nextReady();
    assertNotNull(result);
    assertEquals(2, resultTwo.numTuples());
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 0));
    assertEquals(expectedSecond, resultTwo.getLong(resultTwo.numColumns() - 1, 1));
    mgaTwo.close();
  }

  @Test
  public void testMultiGroupAvg() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1, 2},
            new PrimitiveAggregatorFactory(3, AggregationOp.AVG));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(expected, result.getDouble(result.numColumns() - 1, 0), 0.000001);
    mga.close();
  }

  @Test
  public void testMultiGroupMin() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(3, AggregationOp.MIN));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(expected, result.getLong(result.numColumns() - 1, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupMax() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(3, AggregationOp.MAX));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(3, result.numColumns());
    assertEquals(expected, result.getLong(2, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupMaxAndMin() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(
                3, new AggregationOp[] {AggregationOp.MAX, AggregationOp.MIN}));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(4, result.numColumns());
    assertEquals(expected, result.getLong(2, 0));
    assertEquals(0, result.getLong(3, 0));
    mga.close();
  }

  @Test
  public void testMultiGroupMaxMultiColumn() throws DbException {
    final int numTuples = 10;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(
                3, new AggregationOp[] {AggregationOp.MAX, AggregationOp.MIN}));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
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
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

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
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(0, AggregationOp.COUNT));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    assertEquals(numTuples, result.getLong(result.numColumns() - 1, 0));
    mga.close();
  }

  /**
   * Finds a collision of a tuple of all integers with the given grouping.
   *
   * @param numCols the columns to group by
   * @param groupCols the number of columns in the tuples
   * @return two rows that should have the same hash value
   */
  @SuppressWarnings("unused")
  private static TupleBatch findIntsHashCollision(final int numCols, final int[] groupCols) {
    for (int i : groupCols) {
      assertTrue(i < numCols);
    }
    List<Type> types = new LinkedList<>();
    for (int i = 0; i < numCols; ++i) {
      types.add(Type.INT_TYPE);
    }
    Schema schema = new Schema(types);

    TupleBuffer buffer = new TupleBuffer(schema);
    final Map<Integer, Integer> foundSoFar = new HashMap<>();
    for (int i = 0; i < Integer.MAX_VALUE; ++i) {
      for (int j = 0; j < numCols; ++j) {
        buffer.putInt(j, i);
      }
      int hashCode = HashUtils.hashSubRow(buffer, groupCols, i);
      Integer old = foundSoFar.put(hashCode, i);
      if (old != null) {
        /* Found a match */
        TupleBatchBuffer ret = new TupleBatchBuffer(schema);
        for (int j = 0; j < numCols; ++j) {
          ret.putInt(j, old);
        }
        for (int j = 0; j < numCols; ++j) {
          ret.putInt(j, i);
        }
        return ret.popAny();
      }
    }
    throw new IllegalStateException(
        "Could not find a collision for hashColumns=" + Arrays.toString(groupCols));
  }

  @Test
  public void testMultiGroupCountHashCollision() throws DbException {
    int groupCols[] = new int[] {2, 0};

    /* I used the following code to compute these two collision values. */
    // TupleBatch collision = findIntsHashCollision(3, groupCols);
    // System.err.println(collision.getInt(0, 0)); // 94328
    // System.err.println(collision.getInt(1, 1)); // 113814

    Schema schema = Schema.ofFields(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE);
    TupleBuffer buffer = new TupleBuffer(schema);
    /* First row */
    buffer.putInt(0, 113814);
    buffer.putInt(1, 113814);
    buffer.putInt(2, 113814);
    /* Second row */
    buffer.putInt(0, 3);
    buffer.putInt(1, 5);
    buffer.putInt(2, 4);
    /* Third row */
    buffer.putInt(0, 94328);
    buffer.putInt(1, 94328);
    buffer.putInt(2, 94328);
    /* Fourth row */
    buffer.putInt(0, 113814);
    buffer.putInt(1, 113814);
    buffer.putInt(2, 113814);
    /* Fifth row */
    buffer.putInt(0, 113814);
    buffer.putInt(1, 113814);
    buffer.putInt(2, 113814);
    /* Verify that the collisions hold where expected. */
    assertEquals(
        HashUtils.hashSubRow(buffer, groupCols, 0), HashUtils.hashSubRow(buffer, groupCols, 2));
    assertEquals(
        HashUtils.hashSubRow(buffer, groupCols, 0), HashUtils.hashSubRow(buffer, groupCols, 3));
    assertEquals(
        HashUtils.hashSubRow(buffer, groupCols, 0), HashUtils.hashSubRow(buffer, groupCols, 4));
    /* Verify that collisions do not hold where expected. */
    assertNotEquals(
        HashUtils.hashSubRow(buffer, groupCols, 0), HashUtils.hashSubRow(buffer, groupCols, 1));

    BatchTupleSource source = new BatchTupleSource(buffer.finalResult());
    Aggregate mga =
        new Aggregate(source, groupCols, new PrimitiveAggregatorFactory(1, AggregationOp.COUNT));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNotNull(result);
    assertEquals(3, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    // 113814 3 times
    assertEquals(113814, result.getInt(0, 0));
    assertEquals(113814, result.getInt(1, 0));
    assertEquals(3, result.getLong(2, 0));
    // random vals once
    assertEquals(4, result.getInt(0, 1));
    assertEquals(3, result.getInt(1, 1));
    assertEquals(1, result.getLong(2, 1));
    // 94328 once
    assertEquals(94328, result.getInt(0, 2));
    assertEquals(94328, result.getInt(1, 2));
    assertEquals(1, result.getLong(2, 2));
    mga.close();
  }

  @Test
  public void testMultiGroupCountMultiColumnEmpty() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("a", "b", "c", "d"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    Aggregate mga =
        new Aggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(0, AggregationOp.COUNT));
    mga.open(null);
    TupleBatch result = mga.nextReady();
    assertNull(result);
    mga.close();
  }

  @Test(expected = ArithmeticException.class)
  public void testLongAggOverflow() throws Exception {
    LongColumnBuilder builder =
        new LongColumnBuilder().appendLong(Long.MAX_VALUE - 1).appendLong(3);
    doAggOpsToCol(builder, new AggregationOp[] {AggregationOp.SUM}, true);
  }

  @Test(expected = ArithmeticException.class)
  public void testLongAggUnderflow() throws Exception {
    LongColumnBuilder builder =
        new LongColumnBuilder().appendLong(Long.MIN_VALUE + 1).appendLong(-3);
    doAggOpsToCol(builder, new AggregationOp[] {AggregationOp.SUM}, true);
  }
}
