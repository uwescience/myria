package edu.washington.escience.myria.systemtest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class IterativeSelfJoinTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 400;
  private final int numIteration = 4;
  private final int numTbl1Worker1 = 2000;
  private final int numTbl1Worker2 = 2000;

  public TupleBatchBuffer getResultInMemory(
      final TupleBatchBuffer table1, final Schema schema, final int numIteration) {
    // a brute force check

    final Iterator<List<? extends Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    final boolean graph[][] = new boolean[MaxID][MaxID];
    final boolean cntgraph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      final List<? extends Column<?>> output = tbs.next();
      final int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        final int fr = Integer.parseInt(output.get(0).getObject(i).toString());
        final int fe = Integer.parseInt(output.get(1).getObject(i).toString());
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
          result.putLong(0, i);
          result.putLong(1, j);
          LOGGER.trace(i + "\t" + j);
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
      tbl1Worker1.putLong(0, tbl1ID1Worker1[i]);
      tbl1Worker1.putLong(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.putLong(0, tbl1ID1Worker2[i]);
      tbl1Worker2.putLong(1, tbl1ID2Worker2[i]);
    }
    final TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);

    // generate correct answer in memory
    final TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, numIteration);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    ArrayList<RelationKey> testtableKeys = new ArrayList<RelationKey>(numIteration);

    // database generation
    for (int i = 0; i < numIteration; ++i) {
      testtableKeys.add(RelationKey.of("test", "test", "testtable" + i));
      createTable(workerIDs[0], testtableKeys.get(i), "follower long, followee long");
      createTable(workerIDs[1], testtableKeys.get(i), "follower long, followee long");
    }
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insert(workerIDs[0], testtableKeys.get(i), tableSchema, tb);
      }
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      for (int i = 0; i < numIteration; ++i) {
        insert(workerIDs[1], testtableKeys.get(i), tableSchema, tb);
      }
    }

    // parallel query generation, duplicate db files
    final DbQueryScan scan1 = new DbQueryScan(testtableKeys.get(0), tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(testtableKeys.get(0), tableSchema);

    final int numPartition = 2;
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);

    ArrayList<RootOperator> subqueries = new ArrayList<RootOperator>();
    final GenericShuffleProducer sp0[] = new GenericShuffleProducer[numIteration];
    final GenericShuffleProducer sp1[] = new GenericShuffleProducer[numIteration];
    final GenericShuffleProducer sp2[] = new GenericShuffleProducer[numIteration];
    final GenericShuffleConsumer sc0[] = new GenericShuffleConsumer[numIteration];
    final GenericShuffleConsumer sc1[] = new GenericShuffleConsumer[numIteration];
    final GenericShuffleConsumer sc2[] = new GenericShuffleConsumer[numIteration];
    final SymmetricHashJoin localjoin[] = new SymmetricHashJoin[numIteration];
    final StreamingStateWrapper dupelim[] = new StreamingStateWrapper[numIteration];
    final DbQueryScan scan[] = new DbQueryScan[numIteration];
    ExchangePairID arrayID1, arrayID2, arrayID0;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();

    sp1[0] =
        new GenericShuffleProducer(scan1, arrayID1, new int[] {workerIDs[0], workerIDs[1]}, pf1);
    sp2[0] =
        new GenericShuffleProducer(scan2, arrayID2, new int[] {workerIDs[0], workerIDs[1]}, pf0);
    subqueries.add(sp1[0]);
    subqueries.add(sp2[0]);

    for (int i = 1; i < numIteration; ++i) {

      sc1[i] =
          new GenericShuffleConsumer(
              sp1[i - 1].getSchema(), arrayID1, new int[] {workerIDs[0], workerIDs[1]});
      sc2[i] =
          new GenericShuffleConsumer(
              sp2[i - 1].getSchema(), arrayID2, new int[] {workerIDs[0], workerIDs[1]});
      localjoin[i] =
          new SymmetricHashJoin(
              sc1[i], sc2[i], new int[] {1}, new int[] {0}, new int[] {0}, new int[] {1});
      arrayID0 = ExchangePairID.newID();

      sp0[i] =
          new GenericShuffleProducer(
              localjoin[i], arrayID0, new int[] {workerIDs[0], workerIDs[1]}, pf0);
      subqueries.add(sp0[i]);
      sc0[i] =
          new GenericShuffleConsumer(
              sp0[i].getSchema(), arrayID0, new int[] {workerIDs[0], workerIDs[1]});
      dupelim[i] = new StreamingStateWrapper(sc0[i], new DupElim());
      if (i == numIteration - 1) {
        break;
      }
      scan[i] = new DbQueryScan(testtableKeys.get(i), tableSchema);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();

      sp1[i] =
          new GenericShuffleProducer(
              scan[i], arrayID1, new int[] {workerIDs[0], workerIDs[1]}, pf1);
      sp2[i] =
          new GenericShuffleProducer(
              dupelim[i], arrayID2, new int[] {workerIDs[0], workerIDs[1]}, pf0);
      subqueries.add(sp1[i]);
      subqueries.add(sp2[i]);
    }
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp =
        new CollectProducer(dupelim[numIteration - 1], serverReceiveID, MASTER_ID);
    subqueries.add(cp);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], subqueries.toArray(new RootOperator[subqueries.size()]));
    workerPlans.put(workerIDs[1], subqueries.toArray(new RootOperator[subqueries.size()]));

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] {workerIDs[0], workerIDs[1]});
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
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
