package edu.washington.escience.myria.scaling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConsistentHashTest {

  private static final int NUM_REPLICAS = 3;
  private ConsistentHash consistentHash;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    consistentHash = new ConsistentHash(NUM_REPLICAS);
  }

  /* Dumb test only checking for the number of intervals per worker. */
  @Test
  public void testGetIntervals() {
    List<Integer> workers = new ArrayList<Integer>();
    workers.add(0);
    workers.add(1);
    workers.add(2);

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
    fail("Not yet implemented");
  }

}
