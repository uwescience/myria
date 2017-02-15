package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.joda.time.DateTime;
import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregatorFactory;
import edu.washington.escience.myria.operator.agg.StreamingAggregate;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * Test cases for {@link StreamingAggregate} class. Source tuples are generated in sorted order on group keys, if any.
 * Some of the tests are taken from those for {@link SingleGroupByAggregate} and {@link Aggregate} since
 * StreamingAggregate is expected to behave the same way they do if input is sorted.
 */
public class StreamingAggTest {

  /**
   * Construct a TupleBatchBuffer to be used as source of aggregate. Fixed schema and sorted on grouping columns.
   *
   * @param numTuples number of tuples to be added
   * @return filled TupleBatchBuffer with each group key having (numTuples/10) tuples
   */
  private TupleBatchBuffer fillInputTbb(final int numTuples) {
    final Schema schema =
        Schema.ofFields(
            Type.INT_TYPE,
            "Int",
            Type.DOUBLE_TYPE,
            "Double",
            Type.FLOAT_TYPE,
            "Float",
            Type.LONG_TYPE,
            "Long",
            Type.DATETIME_TYPE,
            "Datetime",
            Type.STRING_TYPE,
            "String",
            Type.BOOLEAN_TYPE,
            "Boolean",
            Type.LONG_TYPE,
            "value");

    final TupleBatchBuffer source = new TupleBatchBuffer(schema);
    for (int i = 0; i < numTuples; i++) {
      int value = i / (numTuples / 10);
      source.putInt(0, value);
      source.putDouble(1, value);
      source.putFloat(2, value);
      source.putLong(3, value);
      source.putDateTime(4, new DateTime(2010 + value, 1, 1, 0, 0));
      source.putString(5, "" + value);
      source.putBoolean(6, (i / (numTuples / 2) == 0));
      source.putLong(7, 2L);
    }
    return source;
  }

  @Test
  public void testSingleGroupKeySingleColumnCount() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over). */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {0},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {1},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {2},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {3},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {4},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {5},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(5, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col6
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {6},
            new PrimitiveAggregatorFactory(7, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(25, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();
  }

  @Test
  public void testSingleGroupKeySingleColumnSum() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over). */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {0},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {1},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {2},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {3},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {4},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {5},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col6
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {6},
            new PrimitiveAggregatorFactory(7, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(50L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();
  }

  @Test
  public void testSingleGroupKeySingleColumnAvg() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over). */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {0},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {1},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {2},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {3},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {4},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {5},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col6
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {6},
            new PrimitiveAggregatorFactory(7, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(2L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();
  }

  @Test
  public void testSingleGroupKeySingleColumnStdev() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over). */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {0},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {1},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {2},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {3},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {4},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {5},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(numTuples / (numTuples / 10), result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col6
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {6},
            new PrimitiveAggregatorFactory(7, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();
  }

  @Test
  public void testSingleGroupKeySingleColumnMin() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over) constant value of 2L. */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col7, agg over col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(0, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0, result.getInt(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col7, agg over col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(1, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col7, agg over col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(2, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0, result.getFloat(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col7, agg over col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(3, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col4, agg over col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(4, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(new DateTime(2010, 1, 1, 0, 0), result.getDateTime(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col7, agg over col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(5, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals("0", result.getString(result.numColumns() - 1, i));
    }
    agg.close();

    // Note: Min not applicable to Boolean type
  }

  @Test
  public void testSingleGroupKeySingleColumnMax() throws DbException {
    final int numTuples = 50;
    /* col0: Int, col1: Double, col2: Float, col3: Long, col4: Datetime, col5: String, col6: Boolean, (first 7 columns
     * used for grouping) col7: Long (to agg over) constant value of 2L. */
    TupleBatchBuffer source = fillInputTbb(numTuples);

    // group by col7, agg over col0
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(0, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(9, result.getInt(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col7, agg over col1
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(1, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(9, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col7, agg over col2
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(2, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(9, result.getFloat(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();

    // group by col7, agg over col3
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(3, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(9L, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col4, agg over col4
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(4, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(new DateTime(2019, 1, 1, 0, 0), result.getDateTime(result.numColumns() - 1, i));
    }
    agg.close();

    // group by col7, agg over col5
    agg =
        new StreamingAggregate(
            new BatchTupleSource(source),
            new int[] {7},
            new PrimitiveAggregatorFactory(5, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals("9", result.getString(result.numColumns() - 1, i));
    }
    agg.close();

    // Note: Max not applicable to type Boolean
  }

  @Test
  public void testMultiGroupSingleColumnCount() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets i
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, i);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();
  }

  @Test
  public void testMultiGroupSingleColumnMin() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets i
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, i);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.MIN));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    assertEquals(0, result.getLong(result.numColumns() - 1, 0));
    assertEquals(10, result.getLong(result.numColumns() - 1, 1));
    assertEquals(20, result.getLong(result.numColumns() - 1, 2));
    assertEquals(30, result.getLong(result.numColumns() - 1, 3));
    assertEquals(40, result.getLong(result.numColumns() - 1, 4));
    agg.close();
  }

  @Test
  public void testMultiGroupSingleColumnMax() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets i
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, i);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.MAX));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    assertEquals(9, result.getLong(result.numColumns() - 1, 0));
    assertEquals(19, result.getLong(result.numColumns() - 1, 1));
    assertEquals(29, result.getLong(result.numColumns() - 1, 2));
    assertEquals(39, result.getLong(result.numColumns() - 1, 3));
    assertEquals(49, result.getLong(result.numColumns() - 1, 4));
    agg.close();
  }

  @Test
  public void testMultiGroupSingleColumnSum() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets 10
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, 10L);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.SUM));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(100, result.getLong(result.numColumns() - 1, i));
    }
    agg.close();
  }

  @Test
  public void testMultiGroupSingleColumnAvg() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets 10
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, 10L);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.AVG));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(10, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();
  }

  @Test
  public void testMultiGroupSingleColumnStdev() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // g0 same for all tuples, g1 split to 5 groups, g2 gets 10
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i / (numTuples / 5));
      tbb.putLong(2, 10L);
    }
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(3, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      assertEquals(0, result.getDouble(result.numColumns() - 1, i), 0.0001);
    }
    agg.close();
  }

  @Test
  public void testSingleGroupKeyMultiColumnAllAgg() throws DbException {
    final int numTuples = 50;
    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "gkey", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // gkey split to 5 groups, value gets 10
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, i / (numTuples / 5));
      tbb.putLong(1, 10L);
    }
    // group by gkey; min on gkey, max on gkey, count on value, sum on value, avg on value, stdev on value
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0},
            new PrimitiveAggregatorFactory(0, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(0, AggregationOp.MAX),
            new PrimitiveAggregatorFactory(1, AggregationOp.COUNT),
            new PrimitiveAggregatorFactory(1, AggregationOp.SUM),
            new PrimitiveAggregatorFactory(1, AggregationOp.AVG),
            new PrimitiveAggregatorFactory(1, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(5, result.numTuples());
    assertEquals(7, result.getSchema().numColumns());
    for (int i = 0; i < result.numTuples(); i++) {
      // min
      assertEquals(result.getLong(0, i), result.getLong(1, i));
      // max
      assertEquals(result.getLong(0, i), result.getLong(2, i));
      // count
      assertEquals(10, result.getLong(3, i));
      // sum
      assertEquals(100, result.getLong(4, i));
      // avg
      assertEquals(10, result.getDouble(5, i), 0.0001);
      // stdev
      assertEquals(0, result.getDouble(6, i), 0.0001);
    }
    agg.close();
  }

  @Test
  public void testMultiGroupMultiColumn() throws DbException {
    final int numTuples = 50;
    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // {0, 2, i} on first half tuples, {0, 4, i} on the second half
    int sumFirst = 0;
    int sumSecond = 0;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      if (i / (numTuples / 2) == 0) {
        tbb.putLong(1, 2L);
        sumFirst += i;
      } else {
        tbb.putLong(1, 4L);
        sumSecond += i;
      }
      tbb.putLong(2, i);
    }

    /* Generate expected values for mean and stdev */
    double meanFirst = (double) sumFirst / (numTuples / 2);
    double meanSecond = (double) sumSecond / (numTuples / 2);
    double diffSquaredFirst = 0.0;
    double diffSquaredSecond = 0.0;
    for (int i = 0; i < numTuples; ++i) {
      if (i / (numTuples / 2) == 0) {
        double diff = i - meanFirst;
        diffSquaredFirst += diff * diff;
      } else {
        double diff = i - meanSecond;
        diffSquaredSecond += diff * diff;
      }
    }
    double expectedFirstStdev = Math.sqrt(diffSquaredFirst / (numTuples / 2));
    double expectedSecondStdev = Math.sqrt(diffSquaredSecond / (numTuples / 2));

    // group by col0 and col1, then min max count sum avg stdev
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(2, AggregationOp.MAX),
            new PrimitiveAggregatorFactory(2, AggregationOp.COUNT),
            new PrimitiveAggregatorFactory(2, AggregationOp.SUM),
            new PrimitiveAggregatorFactory(2, AggregationOp.AVG),
            new PrimitiveAggregatorFactory(2, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(2, result.numTuples());
    assertEquals(8, result.getSchema().numColumns());
    // min
    assertEquals(0, result.getLong(2, 0));
    assertEquals(25, result.getLong(2, 1));
    // max
    assertEquals(24, result.getLong(3, 0));
    assertEquals(49, result.getLong(3, 1));
    // count
    assertEquals(numTuples / 2, result.getLong(4, 0));
    assertEquals(numTuples / 2, result.getLong(4, 1));
    // sum
    assertEquals(sumFirst, result.getLong(5, 0));
    assertEquals(sumSecond, result.getLong(5, 1));
    // avg
    assertEquals(meanFirst, result.getDouble(6, 0), 0.0001);
    assertEquals(meanSecond, result.getDouble(6, 1), 0.0001);
    // stdev
    assertEquals(expectedFirstStdev, result.getDouble(7, 0), 0.0001);
    assertEquals(expectedSecondStdev, result.getDouble(7, 1), 0.0001);
    agg.close();
  }

  @Test
  public void testSingleGroupAllAggLargeInput() throws DbException {

    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "gkey", Type.LONG_TYPE, "value");
    final int numTuples = 2 * TupleUtils.getBatchSize(schema);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    // {0, i}
    int sum = 0;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      tbb.putLong(1, i);
      sum += i;
    }

    /* Generate expected values for mean and stdev */
    double mean = (double) sum / numTuples;
    double diffSquared = 0.0;
    for (int i = 0; i < numTuples; ++i) {
      double diff = i - mean;
      diffSquared += diff * diff;
    }
    double expectedStdev = Math.sqrt(diffSquared / numTuples);

    // group by gkey, then min max count sum avg stdev
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(1, AggregationOp.MAX),
            new PrimitiveAggregatorFactory(1, AggregationOp.COUNT),
            new PrimitiveAggregatorFactory(1, AggregationOp.SUM),
            new PrimitiveAggregatorFactory(1, AggregationOp.AVG),
            new PrimitiveAggregatorFactory(1, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(1, result.numTuples());
    assertEquals(7, result.getSchema().numColumns());
    // min
    assertEquals(0, result.getLong(1, 0));
    // max
    assertEquals(19999, result.getLong(2, 0));
    // count
    assertEquals(numTuples, result.getLong(3, 0));
    // sum
    assertEquals(sum, result.getLong(4, 0));
    // avg
    assertEquals(mean, result.getDouble(5, 0), 0.0001);
    // stdev
    assertEquals(expectedStdev, result.getDouble(6, 0), 0.0001);
    agg.close();
  }

  @Test
  public void testMultiGroupAllAggLargeInput() throws DbException {

    final Schema schema =
        Schema.ofFields(Type.LONG_TYPE, "g0", Type.LONG_TYPE, "g1", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    final int numTuples = 3 * TupleUtils.getBatchSize(schema);
    // split into 4 groups, each group may spread across different batches
    // {0, 0, i} in first group, {0, 1, i} in second, {0, 2, i} in third, {0, 3, i} in fourth
    int sumFirst = 0;
    int sumSecond = 0;
    int sumThird = 0;
    int sumFourth = 0;
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, 0L);
      if (i / (numTuples / 4) == 0) {
        tbb.putLong(1, 0L);
        sumFirst += i;
      } else if (i / (numTuples / 4) == 1) {
        tbb.putLong(1, 1L);
        sumSecond += i;
      } else if (i / (numTuples / 4) == 2) {
        tbb.putLong(1, 2L);
        sumThird += i;
      } else {
        tbb.putLong(1, 3L);
        sumFourth += i;
      }
      tbb.putLong(2, i);
    }

    /* Generate expected values for mean and stdev */
    double meanFirst = (double) sumFirst / (numTuples / 4);
    double meanSecond = (double) sumSecond / (numTuples / 4);
    double meanThird = (double) sumThird / (numTuples / 4);
    double meanFourth = (double) sumFourth / (numTuples / 4);
    double diffSquaredFirst = 0.0;
    double diffSquaredSecond = 0.0;
    double diffSquaredThird = 0.0;
    double diffSquaredFourth = 0.0;
    for (int i = 0; i < numTuples; ++i) {
      if (i / (numTuples / 4) == 0) {
        double diff = i - meanFirst;
        diffSquaredFirst += diff * diff;
      } else if (i / (numTuples / 4) == 1) {
        double diff = i - meanSecond;
        diffSquaredSecond += diff * diff;
      } else if (i / (numTuples / 4) == 2) {
        double diff = i - meanThird;
        diffSquaredThird += diff * diff;
      } else {
        double diff = i - meanFourth;
        diffSquaredFourth += diff * diff;
      }
    }
    double expectedFirstStdev = Math.sqrt(diffSquaredFirst / (numTuples / 4));
    double expectedSecondStdev = Math.sqrt(diffSquaredSecond / (numTuples / 4));
    double expectedThirdStdev = Math.sqrt(diffSquaredThird / (numTuples / 4));
    double expectedFourthStdev = Math.sqrt(diffSquaredFourth / (numTuples / 4));

    // group by col0 and col1, then min max count sum avg stdev
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0, 1},
            new PrimitiveAggregatorFactory(2, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(2, AggregationOp.MAX),
            new PrimitiveAggregatorFactory(2, AggregationOp.COUNT),
            new PrimitiveAggregatorFactory(2, AggregationOp.SUM),
            new PrimitiveAggregatorFactory(2, AggregationOp.AVG),
            new PrimitiveAggregatorFactory(2, AggregationOp.STDEV));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(4, result.numTuples());
    assertEquals(8, result.getSchema().numColumns());
    // min
    assertEquals(0, result.getLong(2, 0));
    assertEquals((numTuples / 4), result.getLong(2, 1));
    assertEquals(2 * (numTuples / 4), result.getLong(2, 2));
    assertEquals(3 * (numTuples / 4), result.getLong(2, 3));
    // max
    assertEquals((numTuples / 4) - 1, result.getLong(3, 0));
    assertEquals(2 * (numTuples / 4) - 1, result.getLong(3, 1));
    assertEquals(3 * (numTuples / 4) - 1, result.getLong(3, 2));
    assertEquals(numTuples - 1, result.getLong(3, 3));
    // count
    assertEquals(numTuples / 4, result.getLong(4, 0));
    assertEquals(numTuples / 4, result.getLong(4, 1));
    assertEquals(numTuples / 4, result.getLong(4, 2));
    assertEquals(numTuples - 3 * (numTuples / 4), result.getLong(4, 3));
    // sum
    assertEquals(sumFirst, result.getLong(5, 0));
    assertEquals(sumSecond, result.getLong(5, 1));
    assertEquals(sumThird, result.getLong(5, 2));
    assertEquals(sumFourth, result.getLong(5, 3));
    // avg
    assertEquals(meanFirst, result.getDouble(6, 0), 0.0001);
    assertEquals(meanSecond, result.getDouble(6, 1), 0.0001);
    assertEquals(meanThird, result.getDouble(6, 2), 0.0001);
    assertEquals(meanFourth, result.getDouble(6, 3), 0.0001);
    // stdev
    assertEquals(expectedFirstStdev, result.getDouble(7, 0), 0.0001);
    assertEquals(expectedSecondStdev, result.getDouble(7, 1), 0.0001);
    assertEquals(expectedThirdStdev, result.getDouble(7, 2), 0.0001);
    assertEquals(expectedFourthStdev, result.getDouble(7, 3), 0.0001);
    agg.close();
  }

  @Test
  public void testMultiBatchResult() throws DbException {

    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "gkey", Type.LONG_TYPE, "value");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    final int numTuples = 3 * TupleUtils.getBatchSize(schema) + 3;
    final int batchSize = TupleUtils.getBatchSize(schema);
    // gkey: 0, 1, 2, ..., numTuples-1; value: 1, 1, 1, ...
    for (long i = 0; i < numTuples; i++) {
      tbb.putLong(0, i);
      tbb.putLong(1, 1L);
    }
    // group by col0, count
    StreamingAggregate agg =
        new StreamingAggregate(
            new BatchTupleSource(tbb),
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.COUNT));
    agg.open(TestEnvVars.get());
    TupleBatch result = agg.nextReady();
    assertNotNull(result);
    assertEquals(batchSize, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    // aggregator should return filled tuple batch, even if it hasn't finished processing all input
    assertFalse(agg.getChild().eos());
    // get second tuple batch
    result = agg.nextReady();
    assertEquals(batchSize, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    assertFalse(agg.getChild().eos());
    // get third tuple batch
    result = agg.nextReady();
    assertEquals(batchSize, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    assertFalse(agg.getChild().eos());
    // get last, non-filled tuple batch
    result = agg.nextReady();
    assertEquals(3, result.numTuples());
    assertEquals(2, result.getSchema().numColumns());
    // child reaches eos()
    assertTrue(agg.getChild().eos());
    // exhaust aggregator
    result = agg.nextReady();
    assertNull(result);
    assertTrue(agg.eos());
    agg.close();
  }
}
