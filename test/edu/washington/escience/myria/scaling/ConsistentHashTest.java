package edu.washington.escience.myria.scaling;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConsistentHashTest {

  private static final int NUM_REPLICAS = 3;
  private static final Integer[] WORKERS = { 0, 1, 2 };
  private static final Integer[] DATAPOINTS = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  private ConsistentHash consistentHash;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    ConsistentHash.initialize(NUM_REPLICAS);
    consistentHash = ConsistentHash.getInstance();
  }

  /* Dumb test only checking for the number of intervals per worker. */
  @Test
  public void testGetIntervals() {
    List<Integer> workers = Arrays.asList(WORKERS);
    for (int i = 0; i < workers.size(); i++) {
      int workerId = workers.get(i);
      System.out.println("Adding node: " + i);
      consistentHash.addWorker(workerId);
      Set<ConsistentHashInterval> intervals = consistentHash.getIntervals(workerId);
      /* We should get NUM_REPLICA * (i + 1) intervals for each of the workers added. */
      assertEquals(NUM_REPLICAS, intervals.size());
    }
  }

  @Test
  public void testAddIntData() {
    // First, construct the consistent hash ring.
    testGetIntervals();
    List<Integer> datapoints = Arrays.asList(DATAPOINTS);
    int[] result = new int[WORKERS.length];
    for (Integer datapoint : datapoints) {
      int workerId = consistentHash.addInt(datapoint);
      result[workerId]++;
    }
    assertEquals(3, result[0]);
    assertEquals(6, result[1]);
    assertEquals(1, result[2]);
  }
}
