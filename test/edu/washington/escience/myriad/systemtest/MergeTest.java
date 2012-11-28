package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.table._TupleBatch;

public class MergeTest extends SystemTestBase {

  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1 = 73;
  private final int numTbl2 = 82;

  @Test
  public void mergeTest() throws DbException, CatalogException, IOException {

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    long[] tbl1ID1 = randomLong(1, MaxID - 1, numTbl1);
    long[] tbl1ID2 = randomLong(1, MaxID - 1, numTbl1);
    long[] tbl2ID1 = randomLong(1, MaxID - 1, numTbl2);
    long[] tbl2ID2 = randomLong(1, MaxID - 1, numTbl2);
    TupleBatchBuffer tbl1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tbl2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1; i++) {
      tbl1.put(0, tbl1ID1[i]);
      tbl1.put(1, tbl1ID2[i]);
    }
    for (int i = 0; i < numTbl2; i++) {
      tbl2.put(0, tbl2ID1[i]);
      tbl2.put(1, tbl2ID2[i]);
    }

    createTable(WORKER_ID[0], "testtable0", "testtable", "follower long, followee long");
    createTable(WORKER_ID[0], "testtable1", "testtable", "follower long, followee long");
    _TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      insertWithBothNames(WORKER_ID[0], "testtable", "testtable0", tableSchema, tb);
    }
    while ((tb = tbl2.popAny()) != null) {
      insertWithBothNames(WORKER_ID[0], "testtable", "testtable1", tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable1.db", "select * from testtable", tableSchema);
    final Merge merge = new Merge(tableSchema, scan1, scan2);
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(merge, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan = new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0] });
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

    // System.out.println(result.numTuples());
    assertTrue(result.numTuples() == (numTbl1 + numTbl2));
  }
}
