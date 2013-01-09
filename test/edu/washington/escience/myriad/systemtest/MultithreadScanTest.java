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
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class MultithreadScanTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1Worker1 = 50;

  public TupleBatchBuffer getResultInMemory(TupleBatchBuffer table1, Schema schema, int numIteration) {
    // a brute force check

    Iterator<List<Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    boolean cntgraph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      List<Column<?>> output = tbs.next();
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
  public void OneThreadTwoConnectionsTest() throws DbException, CatalogException, IOException {

    // data generation
    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }

    TupleBatchBuffer expectedTBB = getResultInMemory(tbl1Worker1, tableSchema, 2);
    TupleBatchBuffer expectedTBBCopy = new TupleBatchBuffer(tableSchema);
    expectedTBBCopy.merge(expectedTBB);

    createTable(WORKER_ID[0], "testtable0", "testtable", "follower long, followee long");
    createTable(WORKER_ID[1], "testtable0", "testtable", "follower long, followee long");
    // }
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insertWithBothNames(WORKER_ID[0], "testtable", "testtable0", tableSchema, tb);
      insertWithBothNames(WORKER_ID[1], "testtable", "testtable0", tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final LocalJoin localjoin = new LocalJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });
    final Project proj = new Project(new Integer[] { 0, 3 }, localjoin);
    final DupElim de = new DupElim(proj);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(de, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp });

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });

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

    expectedTBB.merge(expectedTBBCopy);
    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), TestUtils.tupleBatchToTupleBag(result));
  }

  @Test
  public void TwoThreadsTwoConnectionsSameDBFileTest() throws DbException, CatalogException, IOException {

    // data generation
    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);

    TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, 2);
    TupleBatchBuffer expectedTBBCopy = getResultInMemory(table1, tableSchema, 2);

    createTable(WORKER_ID[0], "testtable0", "testtable", "follower long, followee long");
    createTable(WORKER_ID[1], "testtable0", "testtable", "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insertWithBothNames(WORKER_ID[0], "testtable", "testtable0", tableSchema, tb);
      insertWithBothNames(WORKER_ID[1], "testtable", "testtable0", tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final LocalJoin localjoin1 = new LocalJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });
    final Project proj1 = new Project(new Integer[] { 0, 3 }, localjoin1);
    final DupElim de1 = new DupElim(proj1);
    final SQLiteQueryScan scan3 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan4 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final LocalJoin localjoin2 = new LocalJoin(joinSchema, scan3, scan4, new int[] { 1 }, new int[] { 0 });
    final Project proj2 = new Project(new Integer[] { 0, 3 }, localjoin2);
    final DupElim de2 = new DupElim(proj2);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column

    ExchangePairID arrayID1, arrayID2;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    ShuffleProducer sp1 = new ShuffleProducer(de1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    ShuffleProducer sp2 = new ShuffleProducer(de2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    ShuffleConsumer sc1 = new ShuffleConsumer(sp1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] });
    ShuffleConsumer sc2 = new ShuffleConsumer(sp2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] });
    Merge merge = new Merge(tableSchema, sc1, sc2);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(merge, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp });

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
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

    expectedTBB.merge(expectedTBBCopy);
    expectedTBB.merge(expectedTBBCopy);
    expectedTBB.merge(expectedTBBCopy);

    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), TestUtils.tupleBatchToTupleBag(result));
  }
}
