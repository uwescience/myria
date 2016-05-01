package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.RightHashCountingJoin;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.Tuple;

public class OperatorTestUsingSQLiteStorage extends SystemTestBase {
  /** The logger for this class. */
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OperatorTestUsingSQLiteStorage.class);

  @Test
  public void unbalancedCountingJoinTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();
    int expectedCount = 0;
    for (Integer t : expectedResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =

    new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final RightHashCountingJoin localjoin = new RightHashCountingJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    EmptySink serverPlan = new EmptySink(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).get();

    TupleBatch tb = null;
    long actual = 0;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        actual += tb.getLong(0, 0);
      }
    }
    assertEquals(expectedCount, actual);
  }

}
