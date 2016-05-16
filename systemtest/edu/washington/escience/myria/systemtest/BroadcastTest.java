package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.FixValuePartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

/**
 *
 * Test Broadcast Operator
 *
 * This test performs a broadcast join. There are two relations, testtable1 and testtable2, distributed among workers.
 * This test program broadcast testtable1 and then join it locally with testtable2. After that, collect operator is used
 * to collect result.
 *
 *
 * @author Shumo Chu (chushumo@cs.washington.edu)
 *
 */
public class BroadcastTest extends SystemTestBase {

  @Test
  public void broadcastTest() throws Exception {

    /* use some tables generated in simpleRandomJoinTestBase */
    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    final ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    /* Reuse tables created in SystemTestBase */
    final RelationKey testtable1Key = RelationKey.of("test", "test", "testtable1");
    final RelationKey testtable2Key = RelationKey.of("test", "test", "testtable2");

    final ExchangePairID broadcastID = ExchangePairID.newID(); // for BroadcastOperator
    final ExchangePairID serverReceiveID = ExchangePairID.newID(); // for CollectOperator

    /* Set producer */
    final DbQueryScan scan1 = new DbQueryScan(testtable1Key, schema);
    final GenericShuffleProducer bp =
        new GenericShuffleProducer(
            scan1,
            broadcastID,
            new int[][] {{0, 1}},
            new int[] {workerIDs[0], workerIDs[1]},
            new FixValuePartitionFunction(0));

    /* Set consumer */
    final GenericShuffleConsumer bs =
        new GenericShuffleConsumer(schema, broadcastID, new int[] {workerIDs[0], workerIDs[1]});

    /* Set collect producer which will send data inner-joined */
    final DbQueryScan scan2 = new DbQueryScan(testtable2Key, schema);

    final ImmutableList<String> outputColumnNames =
        ImmutableList.of("id1", "name1", "id2", "name2");
    final SymmetricHashJoin localjoin =
        new SymmetricHashJoin(outputColumnNames, bs, scan2, new int[] {0}, new int[] {0});

    final CollectProducer cp = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    workerPlans.put(workerIDs[0], new RootOperator[] {cp, bp});
    workerPlans.put(workerIDs[1], new RootOperator[] {cp, bp});

    final CollectConsumer serverCollect =
        new CollectConsumer(cp.getSchema(), serverReceiveID, workerIDs);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();

    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);
  }
}
