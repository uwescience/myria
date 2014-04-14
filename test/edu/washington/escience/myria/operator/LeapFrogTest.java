package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class LeapFrogTest {
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
    NAryOperator join =
        new LeapFrogJoin(children, fieldMap, outputMap, outputColumnNames, new boolean[] { false, false });
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

  @Test
  public void testLeapFrogUsingTwitterTriangularJoin() throws DbException {
    final Schema r_schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("r_x", "r_y"));
    final Schema s_schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("s_y", "s_z"));
    final Schema t_schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("t_z", "t_x"));
    /* read data from files. */
    final String realFilename = Paths.get("testdata", "twitter", "TwitterK.csv").toString();
    FileScan fileScanR = new FileScan(realFilename, r_schema);
    FileScan fileScanS = new FileScan(realFilename, s_schema);
    FileScan fileScanT = new FileScan(realFilename, t_schema);
    /* order the tables. */
    InMemoryOrderBy orderR = new InMemoryOrderBy(fileScanR, new int[] { 0, 1 }, new boolean[] { true, true });
    InMemoryOrderBy orderS = new InMemoryOrderBy(fileScanS, new int[] { 0, 1 }, new boolean[] { true, true });
    InMemoryOrderBy orderT = new InMemoryOrderBy(fileScanT, new int[] { 1, 0 }, new boolean[] { true, true });
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] { { { 0, 0 }, { 2, 1 } }, { { 0, 1 }, { 1, 0 } }, { { 1, 1 }, { 2, 0 } } };
    int[][] outputMap = new int[][] { { 0, 0 }, { 0, 1 }, { 1, 1 } };
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y", "z");
    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(new Operator[] { orderR, orderS, orderT }, fieldMap, outputMap, outputColumnNames,
            new boolean[] { true, true, false });
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
    assertEquals(16826, batches.numTuples());
  }

}
