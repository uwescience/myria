package edu.washington.escience.myria.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.operator.network.distribute.HashPartitionFunction;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class MultiFieldHashPartitionFunctionTest {

  private Random rand; // for randomizing numbers

  @Before
  public void setUp() throws Exception {
    rand = new Random();
  }

  @Test
  public void testMultiFieldPartitionFunction() {
    HashPartitionFunction multiFieldPartitionFunction = new HashPartitionFunction(new int[] {0, 1});
    int numGroups = rand.nextInt(10) + 1;
    int tuplesPerGroup = rand.nextInt(10) + 1;
    multiFieldPartitionFunction.setNumPartitions(numGroups);
    BatchTupleSource source = generateTupleBatchSource(numGroups, tuplesPerGroup);
    try {
      source.open(TestEnvVars.get());
      TupleBatch tb = source.nextReady();
      assertNotNull(tb);
      TupleBatch[] partitions = multiFieldPartitionFunction.partition(tb);
      assertEquals(numGroups, partitions.length);
      int s = 0;
      for (TupleBatch p : partitions) {
        assertTrue(p.numTuples() % tuplesPerGroup == 0);
        s += p.numTuples();
      }
      assertEquals(numGroups * tuplesPerGroup, s);
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Generates a tuple batch source with the following schema: a (int), b (int),
   * c (int)
   */
  private BatchTupleSource generateTupleBatchSource(int numGroups, int tuplesPerGroup) {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE),
            ImmutableList.of("a", "b", "c"));
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < numGroups; i++) {
      for (int j = 0; j < tuplesPerGroup; j++) {
        tbb.putInt(0, i);
        tbb.putInt(1, i + 1);
        tbb.putInt(2, rand.nextInt());
      }
    }
    return new BatchTupleSource(tbb);
  }
}
