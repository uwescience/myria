package edu.washington.escience.myriad.systemtest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class IterativeSelfJoinTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 400;
  private final int numIteration = 4;
  private final int numTbl1Worker1 = 2000;
  private final int numTbl1Worker2 = 2000;

  public TupleBatchBuffer getResultInMemory(final TupleBatchBuffer table1, final Schema schema, final int numIteration) {
    // a brute force check

    final Iterator<List<Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    final boolean graph[][] = new boolean[MaxID][MaxID];
    final boolean cntgraph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      final List<Column<?>> output = tbs.next();
      final int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        final int fr = Integer.parseInt(output.get(0).get(i).toString());
        final int fe = Integer.parseInt(output.get(1).get(i).toString());
        graph[fr][fe] = cntgraph[fr][fe] = true;
      }
    }

    final boolean tmpgraph[][] = new boolean[MaxID][MaxID];
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

    final TupleBatchBuffer result = new TupleBatchBuffer(schema);
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
  public void iterativeSelfJoinTest() throws Exception {
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    final long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    final long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    final TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.put(0, tbl1ID1Worker2[i]);
      tbl1Worker2.put(1, tbl1ID2Worker2[i]);
    }
    final TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    // generate correct answer in memory
    final TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, numIteration);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    ArrayList<RelationKey> testtableKeys = new ArrayList<RelationKey>(numIteration);

    // database generation
    for (int i = 0; i < numIteration; ++i) {
      testtableKeys.add(RelationKey.of("test", "test", "testtable" + i));
      createTable(WORKER_ID[0], testtableKeys.get(i), "follower long, followee long");
      createTable(WORKER_ID[1], testtableKeys.get(i), "follower long, followee long");
    }
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insert(WORKER_ID[0], testtableKeys.get(i), tableSchema, tb);
      }
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insert(WORKER_ID[1], testtableKeys.get(i), tableSchema, tb);
      }
    }

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 =
        new SQLiteQueryScan("select * from " + testtableKeys.get(0).toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
            tableSchema);
    final SQLiteQueryScan scan2 =
        new SQLiteQueryScan("select * from " + testtableKeys.get(0).toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
            tableSchema);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column

    ArrayList<RootOperator> subqueries = new ArrayList<RootOperator>();
    final ShuffleProducer sp0[] = new ShuffleProducer[numIteration];
    final ShuffleProducer sp1[] = new ShuffleProducer[numIteration];
    final ShuffleProducer sp2[] = new ShuffleProducer[numIteration];
    final ShuffleConsumer sc0[] = new ShuffleConsumer[numIteration];
    final ShuffleConsumer sc1[] = new ShuffleConsumer[numIteration];
    final ShuffleConsumer sc2[] = new ShuffleConsumer[numIteration];
    final LocalJoin localjoin[] = new LocalJoin[numIteration];
    final DupElim dupelim[] = new DupElim[numIteration];
    final SQLiteQueryScan scan[] = new SQLiteQueryScan[numIteration];
    ExchangePairID arrayID1, arrayID2, arrayID0;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    sp1[0] = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf1);
    sp2[0] = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf0);
    subqueries.add(sp1[0]);
    subqueries.add(sp2[0]);

    for (int i = 1; i < numIteration; ++i) {
      sc1[i] = new ShuffleConsumer(sp1[i - 1].getSchema(), arrayID1, WORKER_ID);
      sc2[i] = new ShuffleConsumer(sp2[i - 1].getSchema(), arrayID2, WORKER_ID);
      localjoin[i] = new LocalJoin(sc1[i], sc2[i], new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
      arrayID0 = ExchangePairID.newID();
      sp0[i] = new ShuffleProducer(localjoin[i], arrayID0, WORKER_ID, pf0);
      subqueries.add(sp0[i]);
      sc0[i] = new ShuffleConsumer(sp0[i].getSchema(), arrayID0, WORKER_ID);
      dupelim[i] = new DupElim(sc0[i]);
      if (i == numIteration - 1) {
        break;
      }
      scan[i] =
          new SQLiteQueryScan("select * from " + testtableKeys.get(i).toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
              tableSchema);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();
      sp1[i] = new ShuffleProducer(scan[i], arrayID1, WORKER_ID, pf1);
      sp2[i] = new ShuffleProducer(dupelim[i], arrayID2, WORKER_ID, pf0);
      subqueries.add(sp1[i]);
      subqueries.add(sp2[i]);
    }
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim[numIteration - 1], serverReceiveID, MASTER_ID);
    subqueries.add(cp);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], subqueries.toArray(new RootOperator[subqueries.size()]));
    workerPlans.put(WORKER_ID[1], subqueries.toArray(new RootOperator[subqueries.size()]));

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
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

  }
}
