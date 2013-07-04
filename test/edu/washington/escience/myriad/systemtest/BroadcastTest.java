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
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.BroadcastProducer;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

/**
 * 
 * Test Broadcast Operator
 * 
 * @author Shumo Chu (chushumo@cs.washington.edu)
 * 
 */
public class BroadcastTest extends SystemTestBase {

  @Test
  public void broadcastTest() throws Exception {

    /* Create a relation in each worker */
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], testtableKey, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableKey, "id long, name varchar(20)");

    /* Generate two parts of the relation, either of them has 200 rows */
    final String[] names1 = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids1 = TestUtils.randomLong(1000, 1005, names1.length);
    final String[] names2 = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids2 = TestUtils.randomLong(1000, 1005, names2.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb1 = new TupleBatchBuffer(schema);
    final TupleBatchBuffer tbb2 = new TupleBatchBuffer(schema);
    for (int i = 0; i < names1.length; i++) {
      tbb1.put(0, ids1[i]);
      tbb1.put(1, names1[i]);
    }

    for (int i = 0; i < names2.length; i++) {
      tbb2.put(0, ids2[i]);
      tbb2.put(1, names2[i]);
    }

    /* Generate expected result */
    final TupleBatchBuffer resultTBB = new TupleBatchBuffer(schema);
    resultTBB.merge(tbb1);
    resultTBB.merge(tbb2);
    final HashMap<Tuple, Integer> expectedResults = TestUtils.tupleBatchToTupleBag(resultTBB);

    /* Insert part1 to worker0, part2 to worker1 */
    TupleBatch tb = null;
    while ((tb = tbb1.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
    }

    while ((tb = tbb2.popAny()) != null) {
      insert(WORKER_ID[1], testtableKey, schema, tb);
    }

    /* Prepare producers */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final BroadcastProducer bp1 = new BroadcastProducer(scanTable, serverReceiveID, WORKER_ID);
    workerPlans.put(WORKER_ID[0], new RootOperator[] { bp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { bp1 });

    /* TODO begin from here: prepare consumers */
    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, WORKER_ID);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();

    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResults, resultBag);
  }
}
