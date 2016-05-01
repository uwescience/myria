package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.JoinTestUtils;
import edu.washington.escience.myria.util.TestEnvVars;

public class SymmetricHashJoinTest {

  @Test
  public void testSymmetricHashJoin() throws DbException {
    BatchTupleSource left = new BatchTupleSource(JoinTestUtils.leftInput);
    BatchTupleSource right = new BatchTupleSource(JoinTestUtils.rightInput);
    Operator join = new SymmetricHashJoin(left, right, new int[] { 1, 0, 2 }, new int[] { 2, 1, 0 });
    join.open(TestEnvVars.get());
    assertEquals(
        Schema.merge(JoinTestUtils.leftSchema, JoinTestUtils.rightSchema), join.getSchema());
    long count = 0;
    while (!join.eos()) {
      TupleBatch tb = join.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    join.close();
    assertEquals(7L, count);
  }

  @Test(expected = IllegalStateException.class)
  public void testIncompatibleJoinKeys() throws DbException {
    BatchTupleSource left = new BatchTupleSource(JoinTestUtils.leftInput);
    BatchTupleSource right = new BatchTupleSource(JoinTestUtils.rightInput);
    Operator join = new SymmetricHashJoin(left, right, new int[] { 0 }, new int[] { 0 });
    join.open(TestEnvVars.get());
  }
}
