package edu.washington.escience.myria.producer;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TupleRangeSource;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myria.systemtest.SystemTestBase;

public class CollectVariantTest extends SystemTestBase {

  private static final Long NUM_TUPLES = 50 * 1000L * 1000L;
  private static final long TRIALS = 10;

  /**
   * Run a simple test producing NUM_TUPLES tuples from one work and sending to master.
   * 
   * @param isCollect if true, CollectProducer is used. if false, GenericShuffleProducer is used.
   * @return the elapsed time of the query.
   * @throws Exception if there are any exceptions.
   */
  private double runTest(boolean isCollect) throws Exception {
    final TupleRangeSource source = new TupleRangeSource(NUM_TUPLES, Type.BOOLEAN_TYPE);
    RootOperator producer;
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    if (isCollect) {
      producer = new CollectProducer(source, serverReceiveID, MASTER_ID);
    } else {
      producer =
          new GenericShuffleProducer(source, serverReceiveID, new int[] { MASTER_ID }, new RoundRobinPartitionFunction(
              1));
    }

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { producer });

    final CollectConsumer serverCollect =
        new CollectConsumer(source.getSchema(), serverReceiveID, new int[] { workerIDs[0] });
    SinkRoot serverPlan = new SinkRoot(serverCollect);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.sync();
    assertTrue(serverPlan.getCount() == NUM_TUPLES);
    return qf.getQuery().getExecutionStatistics().getQueryExecutionElapse();
  }

  @Test
  public void collectTest() throws Exception {
    long collectTime = 0;
    long shuffleTime = 0;
    /* Warmup the system by issuing one of each to start; don't time it. */
    runTest(true);
    runTest(false);

    for (int i = 0; i < TRIALS; ++i) {
      collectTime += runTest(true);
      shuffleTime += runTest(false);
    }

    LOGGER.error("Collect: {} Shuffle: {}", collectTime, shuffleTime);
  }
}
