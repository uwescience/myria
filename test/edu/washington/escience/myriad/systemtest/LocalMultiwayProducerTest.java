package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.util.TestUtils;

public class LocalMultiwayProducerTest extends SystemTestBase {

  // change configuration here
  private final int MaxID = 1000;
  private final int numTbl1 = 5000;
  private final int numTbl2 = 6000;

  @Test
  public void localMultiwayProducerTest() throws DbException, CatalogException, IOException {

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    TupleBatchBuffer expected = new TupleBatchBuffer(tableSchema);

    long[] tbl1ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    long[] tbl1ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    TupleBatchBuffer tbl1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1; i++) {
      tbl1.put(0, tbl1ID1[i]);
      tbl1.put(1, tbl1ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.put(0, tbl1ID1[i]);
        expected.put(1, tbl1ID2[i]);
      }
    }

    long[] tbl2ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    long[] tbl2ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    TupleBatchBuffer tbl2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl2; i++) {
      tbl2.put(0, tbl2ID1[i]);
      tbl2.put(1, tbl2ID2[i]);
      for (int j = 0; j < 2; ++j) {
        expected.put(0, tbl2ID1[i]);
        expected.put(1, tbl2ID2[i]);
      }
    }

    createTable(WORKER_ID[0], "testtable", "follower long, followee long");
    createTable(WORKER_ID[1], "testtable", "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      insert(WORKER_ID[0], "testtable", tableSchema, tb);
    }
    while ((tb = tbl2.popAny()) != null) {
      insert(WORKER_ID[1], "testtable", tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from testtable", tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayProducer multiProducer1 =
        new LocalMultiwayProducer(scan1, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[0]);
    final LocalMultiwayConsumer multiConsumer1_1 =
        new LocalMultiwayConsumer(multiProducer1.getSchema(), consumerID1, new int[] { WORKER_ID[0] });
    final LocalMultiwayConsumer multiConsumer1_2 =
        new LocalMultiwayConsumer(multiProducer1.getSchema(), consumerID2, new int[] { WORKER_ID[0] });
    final LocalMultiwayProducer multiProducer2 =
        new LocalMultiwayProducer(scan1, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[1]);
    final LocalMultiwayConsumer multiConsumer2_1 =
        new LocalMultiwayConsumer(multiProducer2.getSchema(), consumerID1, new int[] { WORKER_ID[1] });
    final LocalMultiwayConsumer multiConsumer2_2 =
        new LocalMultiwayConsumer(multiProducer2.getSchema(), consumerID2, new int[] { WORKER_ID[1] });

    final Merge merge1 = new Merge(tableSchema, multiConsumer1_1, multiConsumer1_2);
    final Merge merge2 = new Merge(tableSchema, multiConsumer2_1, multiConsumer2_2);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp1 = new CollectProducer(merge1, serverReceiveID, MASTER_ID);
    final CollectProducer cp2 = new CollectProducer(merge2, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { multiProducer1, cp1 });
    workerPlans.put(WORKER_ID[1], new Operator[] { multiProducer2, cp2 });

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    TupleBatchBuffer result = null;
    while ((result = Server.runningInstance.startServerQuery(0, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    HashMap<Tuple, Integer> tbag0 = TestUtils.tupleBatchToTupleBag(expected);
    HashMap<Tuple, Integer> tbag1 = TestUtils.tupleBatchToTupleBag(result);
    System.out.println(result.numTuples());
    System.out.println(expected.numTuples());
    TestUtils.assertTupleBagEqual(tbag0, tbag1);

  }
}
