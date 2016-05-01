package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.LocalMultiwayConsumer;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

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
      tbl1.putLong(0, tbl1ID1[i]);
      tbl1.putLong(1, tbl1ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.putLong(0, tbl1ID1[i]);
        expected.putLong(1, tbl1ID2[i]);
      }
    }

    final long[] tbl2ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final long[] tbl2ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final TupleBatchBuffer tbl2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl2; i++) {
      tbl2.putLong(0, tbl2ID1[i]);
      tbl2.putLong(1, tbl2ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.putLong(0, tbl2ID1[i]);
        expected.putLong(1, tbl2ID2[i]);
      }
    }

    createTable(workerIDs[0], testtableKey, "follower long, followee long");
    createTable(workerIDs[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      insert(workerIDs[0], testtableKey, tableSchema, tb);
    }
    while ((tb = tbl2.popAny()) != null) {
      insert(workerIDs[1], testtableKey, tableSchema, tb);
    }

    final DbQueryScan scan1 = new DbQueryScan(testtableKey, tableSchema);
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

    final UnionAll union1 = new UnionAll(new Operator[] { multiConsumer1_1, multiConsumer1_2 });
    final UnionAll union2 = new UnionAll(new Operator[] { multiConsumer2_1, multiConsumer2_2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp1 = new CollectProducer(union1, serverReceiveID, MASTER_ID);
    final CollectProducer cp2 = new CollectProducer(union2, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { multiProducer1, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { multiProducer2, cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    EmptySink serverPlan = new EmptySink(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
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
