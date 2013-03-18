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
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.util.TestUtils;

public class LocalMultiwayProducerTest extends SystemTestBase {

  // change configuration here
  private final int MaxID = 1000;
  private final int numTbl1 = 5000;
  private final int numTbl2 = 6000;

  @Test
  public void localMultiwayProducerTest() throws Exception {

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final TupleBatchBuffer expected = new TupleBatchBuffer(tableSchema);

    final long[] tbl1ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    final long[] tbl1ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    final TupleBatchBuffer tbl1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1; i++) {
      tbl1.put(0, tbl1ID1[i]);
      tbl1.put(1, tbl1ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.put(0, tbl1ID1[i]);
        expected.put(1, tbl1ID2[i]);
      }
    }

    final long[] tbl2ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final long[] tbl2ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final TupleBatchBuffer tbl2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl2; i++) {
      tbl2.put(0, tbl2ID1[i]);
      tbl2.put(1, tbl2ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.put(0, tbl2ID1[i]);
        expected.put(1, tbl2ID2[i]);
      }
    }

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    createTable(WORKER_ID[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
    }
    while ((tb = tbl2.popAny()) != null) {
      insert(WORKER_ID[1], testtableKey, tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("select * from " + testtableKey.toString("sqlite"), tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayProducer multiProducer1 =
        new LocalMultiwayProducer(scan1, new ExchangePairID[] { consumerID1, consumerID2 });
    final LocalMultiwayConsumer multiConsumer1_1 = new LocalMultiwayConsumer(multiProducer1.getSchema(), consumerID1);
    final LocalMultiwayConsumer multiConsumer1_2 = new LocalMultiwayConsumer(multiProducer1.getSchema(), consumerID2);
    final LocalMultiwayProducer multiProducer2 =
        new LocalMultiwayProducer(scan1, new ExchangePairID[] { consumerID1, consumerID2 });
    final LocalMultiwayConsumer multiConsumer2_1 = new LocalMultiwayConsumer(multiProducer2.getSchema(), consumerID1);
    final LocalMultiwayConsumer multiConsumer2_2 = new LocalMultiwayConsumer(multiProducer2.getSchema(), consumerID2);

    final Merge merge1 = new Merge(new Operator[] { multiConsumer1_1, multiConsumer1_2 });
    final Merge merge2 = new Merge(new Operator[] { multiConsumer2_1, multiConsumer2_2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp1 = new CollectProducer(merge1, serverReceiveID, MASTER_ID);
    final CollectProducer cp2 = new CollectProducer(merge2, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { multiProducer1, cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { multiProducer2, cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
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
    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expected), resultBag);

  }
}
