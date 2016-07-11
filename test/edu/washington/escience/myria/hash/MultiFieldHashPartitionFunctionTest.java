package edu.washington.escience.myria.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.network.partition.MultiFieldHashPartitionFunction;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class MultiFieldHashPartitionFunctionTest {

  private static final int NUM_PARTITIONS = 3;
  private Random rand; // for randomizing numbers

  @Before
  public void setUp() throws Exception {
    rand = new Random();
  }

  @Test
  public void testMultiFieldPartitionFunction() {
    MultiFieldHashPartitionFunction multiFieldPartitionFunction =
        new MultiFieldHashPartitionFunction(NUM_PARTITIONS, new int[] {0, 1});
    int numGroups = rand.nextInt(10) + 1;
    int tuplesPerGroup = rand.nextInt(10) + 1;
    TupleSource source = generateTupleBatchSource(numGroups, tuplesPerGroup);
    try {
      source.open(null);
      TupleBatch tb = source.nextReady();
      assertNotNull(tb);
      int[] partitions = multiFieldPartitionFunction.partition(tb);
      // for each of the groups, it must map to the same partition
      for (int i = 0; i < numGroups; i++) {
        int expected = partitions[i * tuplesPerGroup];
        for (int j = 1; j < tuplesPerGroup; j++) {
          assertEquals(expected, partitions[i * tuplesPerGroup + j]);
        }
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Generates a tuple batch source with the following schema: a (int), b (int), c (int)
   */
  private TupleSource generateTupleBatchSource(int numGroups, int tuplesPerGroup) {
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
    return new TupleSource(tbb);
  }
}
