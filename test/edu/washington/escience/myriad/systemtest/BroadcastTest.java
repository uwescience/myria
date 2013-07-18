package edu.washington.escience.myriad.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.QueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.BroadcastConsumer;
import edu.washington.escience.myriad.parallel.BroadcastProducer;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

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
    final QueryScan scan1 = new QueryScan(testtable1Key, schema);
    final BroadcastProducer bp = new BroadcastProducer(scan1, broadcastID, WORKER_ID);

    /* Set consumer */
    final BroadcastConsumer bs = new BroadcastConsumer(schema, broadcastID, WORKER_ID);

    /* Set collect producer which will send data inner-joined */
    final QueryScan scan2 = new QueryScan(testtable2Key, schema);

    final LocalJoin localjoin = new LocalJoin(bs, scan2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, bp });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp, bp });

    final CollectConsumer serverCollect = new CollectConsumer(cp.getSchema(), serverReceiveID, WORKER_ID);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();

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
