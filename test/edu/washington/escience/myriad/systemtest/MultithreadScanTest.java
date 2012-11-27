package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.table._TupleBatch;

public class MultithreadScanTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numIteration = 4;
  private final int numTbl1Worker1 = 60;
  private final int numTbl1Worker2 = 60;

  public TupleBatchBuffer getResultInMemory(TupleBatchBuffer table1, Schema schema, int numIteration) {
    // a brute force check

    Iterator<TupleBatch> tbs = table1.getAll().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    boolean cntgraph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      _TupleBatch tb = tbs.next();
      List<Column<?>> output = tb.outputRawData();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        graph[fr][fe] = cntgraph[fr][fe] = true;
      }
    }

    boolean tmpgraph[][] = new boolean[MaxID][MaxID];
    int cnt = 0;
    while (true) {
      ++cnt;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (graph[j][i] && cntgraph[i][k]) {
              tmpgraph[j][k] = true;
            }
          }
        }
      }
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          cntgraph[i][j] = tmpgraph[i][j];
          tmpgraph[i][j] = false;
        }
      }
      if (cnt == numIteration - 1) {
        break;
      }
    }

    TupleBatchBuffer result = new TupleBatchBuffer(schema);
    for (int i = 0; i < MaxID; ++i) {
      for (int j = 0; j < MaxID; ++j) {
        if (cntgraph[i][j]) {
          result.put(0, (long) i);
          result.put(1, (long) j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  @Test
  public void iterativeSelfJoinTest() throws DbException, CatalogException, IOException {

    // data generation
    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    long[] tbl1ID1Worker1 = randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker1 = randomLong(1, MaxID - 1, numTbl1Worker1);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);

    // generate correct answer in memory
    TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, 2);
    HashMap<Tuple, Integer> expectedResult = SystemTestBase.tupleBatchToTupleBag(expectedTBB);

    // database generation
    // for (int i = 0; i < numIteration; ++i) {
    createTable(WORKER_ID[0], "testtable0", "testtable", "follower long, followee long");
    // }
    _TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      // for (int i = 0; i < numIteration; ++i) {
      insertWithBothNames(WORKER_ID[0], "testtable", "testtable0", tableSchema, tb);
      // }
    }

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final LocalJoin localjoin = new LocalJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);

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
    LOGGER.debug("Query dispatched to the workers");
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

    HashMap<Tuple, Integer> actual = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(expectedResult, actual);
  }
}
