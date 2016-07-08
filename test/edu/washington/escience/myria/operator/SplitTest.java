package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.regex.PatternSyntaxException;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class SplitTest {

  @Test
  public void testGeneratedSplits() throws DbException {
    final Object[][] expectedResults = {
      {true, "foo:bar:baz", 1L, 0.1, "foo"},
      {true, "foo:bar:baz", 1L, 0.1, "bar"},
      {true, "foo:bar:baz", 1L, 0.1, "baz"},
      {false, ":qux::", 2L, 0.2, ""},
      {false, ":qux::", 2L, 0.2, "qux"},
      {false, ":qux::", 2L, 0.2, ""},
      {false, ":qux::", 2L, 0.2, ""}
    };
    final Schema schema =
        Schema.ofFields(
            "bool",
            Type.BOOLEAN_TYPE,
            "string",
            Type.STRING_TYPE,
            "long",
            Type.LONG_TYPE,
            "double",
            Type.DOUBLE_TYPE);
    final Schema expectedResultSchema =
        Schema.appendColumn(schema, Type.STRING_TYPE, "string_splits");
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    // First row to explode
    input.putBoolean(0, true);
    input.putString(1, "foo:bar:baz");
    input.putLong(2, 1L);
    input.putDouble(3, 0.1);
    // Second row to explode
    input.putBoolean(0, false);
    input.putString(1, ":qux::");
    input.putLong(2, 2L);
    input.putDouble(3, 0.2);
    Split splitOp = new Split(new TupleSource(input), 1, ":");

    splitOp.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!splitOp.eos()) {
      TupleBatch result = splitOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(
              ((Boolean) expectedResults[rowIdx][0]).booleanValue(),
              result.getBoolean(0, batchIdx));
          assertEquals((expectedResults[rowIdx][1]).toString(), result.getString(1, batchIdx));
          assertEquals(
              ((Long) expectedResults[rowIdx][2]).longValue(), result.getLong(2, batchIdx));
          assertEquals(
              Double.doubleToLongBits(((Double) expectedResults[rowIdx][3]).doubleValue()),
              Double.doubleToLongBits(result.getDouble(3, batchIdx)));
          assertEquals((expectedResults[rowIdx][4]).toString(), result.getString(4, batchIdx));
        }
      }
    }
    assertEquals(expectedResults.length, rowIdx);
    splitOp.close();
  }

  @Test
  public void testGeneratedSplitsSingleColumn() throws DbException {
    final String[][] expectedResults = {
      {"foo:bar:baz", "foo"}, {"foo:bar:baz", "bar"}, {"foo:bar:baz", "baz"}
    };
    final Schema schema = Schema.ofFields("string", Type.STRING_TYPE);
    final Schema expectedResultSchema =
        Schema.appendColumn(schema, Type.STRING_TYPE, "string_splits");
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    input.putString(0, "foo:bar:baz");
    Split splitOp = new Split(new TupleSource(input), 0, ":");

    splitOp.open(TestEnvVars.get());
    int rowIdx = 0;
    while (!splitOp.eos()) {
      TupleBatch result = splitOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(expectedResults[rowIdx][0], result.getString(0, batchIdx));
          assertEquals(expectedResults[rowIdx][1], result.getString(1, batchIdx));
        }
      }
    }
    assertEquals(expectedResults.length, rowIdx);
    splitOp.close();
  }

  /**
   * Test output spanning multiple batches. All integers from 0 to 2 * TupleBatch.BATCH_SIZE are concatenated as a
   * single comma-separated string. Result should contain each integer from the input in its own row.
   *
   * @throws DbException
   */
  @Test
  public void testAllBatchesReturned() throws DbException {
    final Schema schema = Schema.ofFields("joined_ints", Type.STRING_TYPE);
    final Schema expectedResultSchema =
        Schema.appendColumn(schema, Type.STRING_TYPE, "joined_ints_splits");
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    final long expectedResults = 2 * TupleBatch.BATCH_SIZE + 1;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < expectedResults; ++i) {
      sb.append(i);
      if (i < expectedResults - 1) {
        sb.append(",");
      }
    }
    input.putString(0, sb.toString());

    Split splitOp = new Split(new TupleSource(input), 0, ",");
    splitOp.open(TestEnvVars.get());
    long rowIdx = 0;
    while (!splitOp.eos()) {
      TupleBatch result = splitOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());

        for (int batchIdx = 0; batchIdx < result.numTuples(); ++batchIdx, ++rowIdx) {
          assertEquals(rowIdx, Integer.parseInt(result.getString(1, batchIdx)));
        }
      }
    }
    assertEquals(expectedResults, rowIdx);
    splitOp.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testSplitColumnInvalidType() throws DbException {
    final Schema schema = Schema.ofFields("long", Type.LONG_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    input.putLong(0, 1L);
    Split splitOp = new Split(new TupleSource(input), 0, ":");
    splitOp.open(TestEnvVars.get());
  }

  @Test(expected = PatternSyntaxException.class)
  public void testInvalidRegex() throws DbException {
    final Schema schema = Schema.ofFields("string", Type.STRING_TYPE);
    final TupleBatchBuffer input = new TupleBatchBuffer(schema);
    input.putString(0, "foo");
    Split splitOp = new Split(new TupleSource(input), 0, "?:(");
    splitOp.open(TestEnvVars.get());
  }
}
