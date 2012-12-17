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
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.util.TestUtils;

public class IterativeSelfJoinTest extends SystemTestBase {
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
    System.out.println(System.getProperty("java.util.logging.config.file"));
    // data generation
    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.put(0, tbl1ID1Worker2[i]);
      tbl1Worker2.put(1, tbl1ID2Worker2[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    // generate correct answer in memory
    TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, numIteration);

    HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // database generation
    for (int i = 0; i < numIteration; ++i) {
      createTable(WORKER_ID[0], "testtable" + i, "testtable", "follower long, followee long");
      createTable(WORKER_ID[1], "testtable" + i, "testtable", "follower long, followee long");
    }
    _TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insertWithBothNames(WORKER_ID[0], "testtable", "testtable" + i, tableSchema, tb);
      }
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insertWithBothNames(WORKER_ID[1], "testtable", "testtable" + i, tableSchema, tb);
      }
    }

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable1.db", "select * from testtable", tableSchema);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column

    final ShuffleProducer sp0[] = new ShuffleProducer[numIteration];
    final ShuffleProducer sp1[] = new ShuffleProducer[numIteration];
    final ShuffleProducer sp2[] = new ShuffleProducer[numIteration];
    final ShuffleConsumer sc0[] = new ShuffleConsumer[numIteration];
    final ShuffleConsumer sc1[] = new ShuffleConsumer[numIteration];
    final ShuffleConsumer sc2[] = new ShuffleConsumer[numIteration];
    final LocalJoin localjoin[] = new LocalJoin[numIteration];
    final Project proj[] = new Project[numIteration];
    final DupElim dupelim[] = new DupElim[numIteration];
    final SQLiteQueryScan scan[] = new SQLiteQueryScan[numIteration];
    ExchangePairID arrayID1, arrayID2, arrayID0;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    sp1[0] = new ShuffleProducer(scan1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
    sp2[0] = new ShuffleProducer(scan2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);

    for (int i = 1; i < numIteration; ++i) {
      sc1[i] = new ShuffleConsumer(sp1[i - 1], arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] });
      sc2[i] = new ShuffleConsumer(sp2[i - 1], arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] });
      localjoin[i] = new LocalJoin(joinSchema, sc1[i], sc2[i], new int[] { 1 }, new int[] { 0 });
      proj[i] = new Project(new Integer[] { 0, 3 }, localjoin[i]);
      arrayID0 = ExchangePairID.newID();
      sp0[i] = new ShuffleProducer(proj[i], arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
      sc0[i] = new ShuffleConsumer(sp0[i], arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] });
      dupelim[i] = new DupElim(sc0[i]);
      if (i == numIteration - 1) {
        break;
      }
      scan[i] = new SQLiteQueryScan("testtable" + (i + 1) + ".db", "select * from testtable", tableSchema);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();
      sp1[i] = new ShuffleProducer(scan[i], arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
      sp2[i] = new ShuffleProducer(dupelim[i], arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    }
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim[numIteration - 1], serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp);
    workerPlans.put(WORKER_ID[1], cp);

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
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = null;
    while ((result = Server.runningInstance.startServerQuery(0, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(expectedResult, actual);
  }
}
