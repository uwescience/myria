package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class LeapFrogJoinTest {
  @Test
  public void testLeapFrogJoinOnMultipleTBInBuffer() throws DbException {
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id2", "name2"));
    TupleBatchBuffer leftTbb = new TupleBatchBuffer(schema);
    TupleBatchBuffer rightTbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < 2; ++i) {
      leftTbb.putLong(0, 0);
      leftTbb.putString(1, "hello world");
    }

    for (int i = 0; i < TupleBatch.BATCH_SIZE + 1; ++i) {
      rightTbb.putLong(0, 0);
      rightTbb.putString(1, "hello world");
    }

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(leftTbb);
    children[1] = new TupleSource(rightTbb);

    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE),
            outputColumnNames);

    int[][][] fieldMap = new int[][][] { { { 0, 0 }, { 1, 0 } } };
    int[][] outputMap = new int[][] { { 0, 0 }, { 0, 1 }, { 1, 0 }, { 1, 1 } };
    NAryOperator join = new LeapFrogJoin(children, fieldMap, outputMap, outputColumnNames);
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(2 * (TupleBatch.BATCH_SIZE + 1), batches.numTuples());
  }
}
