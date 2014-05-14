package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.ConsistentHash;
import edu.washington.escience.myria.parallel.ConsistentHash.ConsistentHashInterval;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class ConsistentHashTest {

  @Test
  public void testGetWorkerIdWrapAround() {
    int numIntervals = 10;
    int startRange = 18;
    int step = 2;
    int numReplicas = 0;
    Set<ConsistentHashInterval> intervals =
        generateConsistentHashIntervals(numIntervals, startRange, step, numReplicas);
    ConsistentHash consistentHash = ConsistentHash.of(intervals);
    assertEquals(1, ConsistentHash.getWorkerId(intervals, 0));
    assertEquals(1, consistentHash.getWorkerId(0));
  }

  @Ignore("This is only a debugging test case.")
  @Test
  public void dummy() {
    ConsistentHash consistentHash = ConsistentHash.of(10, 0);
    TupleBatchBuffer tbb = new TupleBatchBuffer(new Schema(ImmutableList.of(Type.INT_TYPE)));
    for (int i = 1; i <= 10; i++) {
      tbb.putInt(0, i);
    }
    TupleBatch tb = tbb.popAny();
    for (int row = 0; row < tb.numTuples(); ++row) {
      int hashCode = tb.hashCode(row, 0);
      System.out.println("workerId wrapped: " + ((consistentHash.getWorkerId(hashCode) + 1) % 3));
    }
  }

  @Test
  public void testGetWorkerIdGeneral() {
    // Test descriptions:
    // - Create intervals size of 2 for 10 intervals without replication.
    // - Each interval belongs to a node, so each of the interval belongs to different nodes.
    // - The range of the circle is [0, 20).
    // - hash a value and it should gives the correct worker index.
    int numIntervals = 10;
    int startRange = 0;
    int step = 2;
    int numReplicas = 0;
    Set<ConsistentHashInterval> intervals =
        generateConsistentHashIntervals(numIntervals, startRange, step, numReplicas);
    ConsistentHash consistentHash = ConsistentHash.of(intervals);
    for (int i = 0; i < numIntervals; i++) {
      for (int j = 1; j <= step; j++) {
        int expected = i;
        assertEquals(expected, ConsistentHash.getWorkerId(intervals, (i * step) + j));
        assertEquals(expected, consistentHash.getWorkerId((i * step) + j));
      }
    }
  }

  @Test
  public void testGetMaxN() {
    int numIntervals = 10;
    int startRange = 0;
    int step = 2;
    int numReplicas = 0;
    Set<ConsistentHashInterval> intervals =
        generateConsistentHashIntervals(numIntervals, startRange, step, numReplicas);
    ConsistentHash consistentHash = ConsistentHash.of(intervals);
    int numWorkers = 3;
    for (int i = 0; i < numIntervals; i++) {
      for (int j = 1; j <= step; j++) {
        int expected = i;
        if (i < numWorkers) {
          expected = numWorkers;
        }
        assertEquals(expected, ConsistentHash.getMaxN(intervals, (i * step) + j, numWorkers));
        assertEquals(expected, consistentHash.getMaxN((i * step) + j, numWorkers));
      }
    }
  }

  /**
   * Construct consistent hash intervals.
   * 
   * @param numIntervals the number of intervals.
   * @param startRange the start range.
   * @param step the step between the hash values.
   * @param numReplicas number of replicas per worker.
   * @return
   */
  private Set<ConsistentHashInterval> generateConsistentHashIntervals(final int numIntervals, final int start,
      final int step, final int numReplicas) {
    Set<ConsistentHashInterval> intervals = new HashSet<ConsistentHashInterval>();
    int startRange = start;
    for (int replica = 0; replica <= numReplicas; replica++) {
      for (int workerId = 0; workerId < numIntervals; workerId++) {
        int end = (startRange + step) % ((step * numIntervals) + 1);
        intervals.add(new ConsistentHashInterval(startRange, end, workerId, replica));
        startRange = (startRange + step) % (step * numIntervals);
      }
    }
    return intervals;
  }

}
