package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class JoinTest {

  public static Schema leftSchema = getLeftSchema();
  public static Schema rightSchema = getRightSchema();
  public static ImmutableList<TupleBatch> leftInput = ImmutableList.copyOf(getLeftInput());
  public static ImmutableList<TupleBatch> rightInput = ImmutableList.copyOf(getRightInput());

  public static Schema getLeftSchema() {
    return Schema.ofFields("left1", Type.LONG_TYPE, "left2", Type.BOOLEAN_TYPE, "left3", Type.STRING_TYPE);
  }

  public static Schema getRightSchema() {
    return Schema.ofFields("right1", Type.STRING_TYPE, "right2", Type.LONG_TYPE, "right3", Type.BOOLEAN_TYPE);
  }

  public static List<TupleBatch> getLeftInput() {
    TupleBatchBuffer tbb = new TupleBatchBuffer(leftSchema);
    List<TupleBatch> ret = Lists.newLinkedList();

    tbb.putLong(0, 10L);
    tbb.putBoolean(1, true);
    tbb.putString(2, "value1");

    tbb.putLong(0, 15L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value2");

    tbb.putLong(0, 11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value");

    ret.add(tbb.popAny());

    tbb.putLong(0, -11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value3");

    tbb.putLong(0, -11L);
    tbb.putBoolean(1, false);
    tbb.putString(2, "value3");

    ret.add(tbb.popAny());

    return ret;
  }

  public static List<TupleBatch> getRightInput() {
    TupleBatchBuffer tbb = new TupleBatchBuffer(rightSchema);
    List<TupleBatch> ret = Lists.newLinkedList();

    tbb.putString(0, "value");
    tbb.putLong(1, 11L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    tbb.putString(0, "value2");
    tbb.putLong(1, 15L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    tbb.putString(0, "value3");
    tbb.putLong(1, -14L);
    tbb.putBoolean(2, false);

    tbb.putString(0, "value1");
    tbb.putLong(1, 10L);
    tbb.putBoolean(2, true);

    tbb.putString(0, "value3");
    tbb.putLong(1, -11L);
    tbb.putBoolean(2, false);

    ret.add(tbb.popAny());

    return ret;
  }

  @Test
  public void testSymmetricHashJoin() throws DbException {
    TupleSource left = new TupleSource(getLeftInput());
    TupleSource right = new TupleSource(getRightInput());
    Operator join = new SymmetricHashJoin(left, right, new int[] { 1, 0, 2 }, new int[] { 2, 1, 0 });
    join.open(TestEnvVars.get());
    assertEquals(Schema.merge(leftSchema, rightSchema), join.getSchema());
    long count = 0;
    while (!join.eos()) {
      TupleBatch tb = join.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    join.close();
    assertEquals(5L, count);
  }

  @Test
  public void testRightHashJoin() throws DbException {
    TupleSource left = new TupleSource(getLeftInput());
    TupleSource right = new TupleSource(getRightInput());
    Operator join = new RightHashJoin(left, right, new int[] { 0, 1, 2 }, new int[] { 1, 2, 0 });
    join.open(TestEnvVars.get());
    assertEquals(Schema.merge(leftSchema, rightSchema), join.getSchema());
    long count = 0;
    while (!join.eos()) {
      TupleBatch tb = join.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    join.close();
    assertEquals(5L, count);
  }

  @Test
  public void testSymmetricHashCountingJoin() throws DbException {
    TupleSource left = new TupleSource(getLeftInput());
    TupleSource right = new TupleSource(getRightInput());
    Operator join = new SymmetricHashCountingJoin(left, right, new int[] { 0, 1, 2 }, new int[] { 1, 2, 0 });
    join.open(TestEnvVars.get());
    assertEquals(1, join.getSchema().numColumns());
    assertEquals(Type.LONG_TYPE, join.getSchema().getColumnType(0));
    long count = 0;
    while (!join.eos()) {
      TupleBatch tb = join.nextReady();
      if (tb == null) {
        continue;
      }
      for (int row = 0; row < tb.numTuples(); ++row) {
        count += tb.getLong(0, row);
      }
    }
    join.close();
    assertEquals(5L, count);
  }
}
