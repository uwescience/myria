package edu.washington.escience.myria.systemtest;

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
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
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

public class MultithreadScanTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1Worker1 = 50;

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
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  @Test
  public void OneThreadTwoConnectionsTest() throws Exception {

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    final long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.putLong(0, tbl1ID1Worker1[i]);
      tbl1Worker1.putLong(1, tbl1ID2Worker1[i]);
    }

    final TupleBatchBuffer expectedTBB = getResultInMemory(tbl1Worker1, tableSchema, 2);
    final TupleBatchBuffer expectedTBBCopy = new TupleBatchBuffer(tableSchema);
    expectedTBBCopy.unionAll(expectedTBB);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    createTable(workerIDs[0], testtableKey, "follower long, followee long");
    createTable(workerIDs[1], testtableKey, "follower long, followee long");
    // }
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], testtableKey, tableSchema, tb);
      insert(workerIDs[1], testtableKey, tableSchema, tb);
    }

    final DbQueryScan scan1 = new DbQueryScan(testtableKey, tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(testtableKey, tableSchema);
    final SymmetricHashJoin localjoin =
        new SymmetricHashJoin(
            scan1, scan2, new int[] {1}, new int[] {0}, new int[] {0}, new int[] {1});
    final StreamingStateWrapper de = new StreamingStateWrapper(localjoin, new DupElim());

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(de, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] {cp});
    workerPlans.put(workerIDs[1], new RootOperator[] {cp});

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] {workerIDs[0], workerIDs[1]});
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    expectedTBB.unionAll(expectedTBBCopy);
    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), resultBag);
  }

  @Test
  public void TwoThreadsTwoConnectionsSameDBFileTest() throws Exception {

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    final long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.putLong(0, tbl1ID1Worker1[i]);
      tbl1Worker1.putLong(1, tbl1ID2Worker1[i]);
    }
    final TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.unionAll(tbl1Worker1);

    final TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, 2);
    final TupleBatchBuffer expectedTBBCopy = getResultInMemory(table1, tableSchema, 2);

    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    createTable(workerIDs[0], testtableKey, "follower long, followee long");
    createTable(workerIDs[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], testtableKey, tableSchema, tb);
      insert(workerIDs[1], testtableKey, tableSchema, tb);
    }

    final DbQueryScan scan1 = new DbQueryScan(testtableKey, tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(testtableKey, tableSchema);
    final SymmetricHashJoin localjoin1 =
        new SymmetricHashJoin(
            scan1, scan2, new int[] {1}, new int[] {0}, new int[] {0}, new int[] {1});
    final StreamingStateWrapper de1 = new StreamingStateWrapper(localjoin1, new DupElim());
    final DbQueryScan scan3 = new DbQueryScan(testtableKey, tableSchema);
    final DbQueryScan scan4 = new DbQueryScan(testtableKey, tableSchema);
    final SymmetricHashJoin localjoin2 =
        new SymmetricHashJoin(
            scan3, scan4, new int[] {1}, new int[] {0}, new int[] {0}, new int[] {1});
    final StreamingStateWrapper de2 = new StreamingStateWrapper(localjoin2, new DupElim());

    final int numPartition = 2;
    final PartitionFunction pf0 =
        new SingleFieldHashPartitionFunction(numPartition, 0); // 2 workers

    ExchangePairID arrayID1, arrayID2;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(de1, arrayID1, new int[] {workerIDs[0], workerIDs[1]}, pf0);
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(de2, arrayID2, new int[] {workerIDs[0], workerIDs[1]}, pf0);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(
            sp1.getSchema(), arrayID1, new int[] {workerIDs[0], workerIDs[1]});
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(
            sp2.getSchema(), arrayID2, new int[] {workerIDs[0], workerIDs[1]});
    final UnionAll unionAll = new UnionAll(new Operator[] {sc1, sc2});

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(unionAll, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] {cp, sp1, sp2});
    workerPlans.put(workerIDs[1], new RootOperator[] {cp, sp1, sp2});

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] {workerIDs[0], workerIDs[1]});
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);

    expectedTBB.unionAll(expectedTBBCopy);
    expectedTBB.unionAll(expectedTBBCopy);
    expectedTBB.unionAll(expectedTBBCopy);
    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), resultBag);
  }
}
