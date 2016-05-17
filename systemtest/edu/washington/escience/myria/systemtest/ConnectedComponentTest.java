package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.KeepMinValue;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.LocalMultiwayConsumer;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class ConnectedComponentTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numTbl2Worker1 = 100;
  private final int numTbl2Worker2 = 150;

  public TupleBatchBuffer getConnectedComponentsResult(
      final TupleBatchBuffer g, final Schema schema) {
    final Iterator<List<? extends Column<?>>> iter = g.getAllAsRawColumn().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    while (iter.hasNext()) {
      List<? extends Column<?>> output = iter.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).getObject(i).toString());
        int fe = Integer.parseInt(output.get(1).getObject(i).toString());
        graph[fr][fe] = true;
      }
    }
    TupleBatchBuffer ans = new TupleBatchBuffer(schema);
    boolean[] visited = new boolean[MaxID];
    for (int i = 0; i < MaxID; ++i) {
      if (visited[i]) {
        continue;
      }
      List<Integer> queue = new ArrayList<Integer>();
      queue.add(i);
      visited[i] = true;
      int index = 0;
      while (index < queue.size()) {
        int cnt = queue.get(index);
        ans.putLong(0, cnt);
        ans.putLong(1, i);

        for (int j = 0; j < MaxID; ++j) {
          if (!visited[j] && graph[cnt][j]) {
            queue.add(j);
            visited[j] = true;
          }
        }
        index++;
      }
    }
    return ans;
  }

  public List<RootOperator> generatePlan(
      final Schema table1Schema,
      final Schema table2Schema,
      final ExchangePairID joinArrayId1,
      final ExchangePairID joinArrayId2,
      final ExchangePairID joinArrayId3,
      final ExchangePairID eoiReceiverOpId,
      final ExchangePairID eosReceiverOpId,
      final ExchangePairID serverOpId,
      final boolean prioritized) {
    final int numPartition = 2;
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);

    // the query plan
    final DbQueryScan scan1 = new DbQueryScan(RelationKey.of("test", "test", "c"), table1Schema);
    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, joinArrayId1, workerIDs, pf0);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(table1Schema, joinArrayId1, workerIDs);
    final GenericShuffleConsumer sc3 =
        new GenericShuffleConsumer(table1Schema, joinArrayId3, workerIDs);
    final Consumer eosReceiver =
        new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpId, new int[] {workerIDs[0]});
    final IDBController idbController =
        new IDBController(
            0,
            eoiReceiverOpId,
            workerIDs[0],
            sc1,
            sc3,
            eosReceiver,
            new KeepMinValue(new int[] {0}, new int[] {1}));

    final DbQueryScan scan2 = new DbQueryScan(RelationKey.of("test", "test", "g"), table2Schema);
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, joinArrayId2, workerIDs, pf1);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(table2Schema, joinArrayId2, workerIDs);

    final ExchangePairID mpId1 = ExchangePairID.newID();
    final ExchangePairID mpId2 = ExchangePairID.newID();
    final LocalMultiwayProducer mp =
        new LocalMultiwayProducer(idbController, new ExchangePairID[] {mpId1, mpId2});
    final LocalMultiwayConsumer mc1 = new LocalMultiwayConsumer(table1Schema, mpId1);
    final LocalMultiwayConsumer mc2 = new LocalMultiwayConsumer(table1Schema, mpId2);
    final SymmetricHashJoin join =
        new SymmetricHashJoin(
            sc2, mc1, new int[] {1}, new int[] {0}, new int[] {0}, new int[] {1}, false, true);
    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(mc2, 0, new SingleColumnAggregatorFactory(1, AggregationOp.MIN));
    final CollectProducer cp = new CollectProducer(agg, serverOpId, MASTER_ID);
    final GenericShuffleProducer sp3 =
        new GenericShuffleProducer(join, joinArrayId3, workerIDs, pf0);
    if (prioritized) {
      sp3.setBackupBufferAsPrioritizedMin(new int[] {0}, new int[] {1});
    } else {
      sp3.setBackupBufferAsMin(new int[] {0}, new int[] {1});
    }

    List<RootOperator> ret = new ArrayList<RootOperator>();
    ret.add(sp1);
    ret.add(sp2);
    ret.add(sp3);
    ret.add(mp);
    ret.add(cp);
    return ret;
  }

  public void connectedComponents(final boolean failure, final boolean prioritized)
      throws Exception {
    if (failure) {
      TestUtils.skipIfInTravis();
      return;
    }

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("node_id", "cluster_id");
    final Schema table1Schema = new Schema(table1Types, table1ColumnNames);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(table1Schema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(table1Schema);
    for (int i = 0; i < MaxID / 2; i++) {
      tbl1Worker1.putLong(0, i);
      tbl1Worker1.putLong(1, i);
    }
    for (int i = MaxID / 2; i < MaxID; i++) {
      tbl1Worker2.putLong(0, i);
      tbl1Worker2.putLong(1, i);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(table1Schema);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);
    createTable(workerIDs[0], RelationKey.of("test", "test", "c"), "node_id long, cluster_id long");
    createTable(workerIDs[1], RelationKey.of("test", "test", "c"), "node_id long, cluster_id long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], RelationKey.of("test", "test", "c"), table1Schema, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(workerIDs[1], RelationKey.of("test", "test", "c"), table1Schema, tb);
    }

    final ImmutableList<Type> table2Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table2ColumnNames = ImmutableList.of("node_id", "node_id_another");
    final Schema table2Schema = new Schema(table2Types, table2ColumnNames);
    long[] tbl2ID1Worker1 = TestUtils.randomLong(0, MaxID - 1, numTbl2Worker1);
    long[] tbl2ID1Worker2 = TestUtils.randomLong(0, MaxID - 1, numTbl2Worker2);
    long[] tbl2ID2Worker1 = TestUtils.randomLong(0, MaxID - 1, numTbl2Worker1);
    long[] tbl2ID2Worker2 = TestUtils.randomLong(0, MaxID - 1, numTbl2Worker2);
    TupleBatchBuffer tbl2Worker1 = new TupleBatchBuffer(table2Schema);
    TupleBatchBuffer tbl2Worker2 = new TupleBatchBuffer(table2Schema);
    for (int i = 0; i < numTbl2Worker1; i++) {
      tbl2Worker1.putLong(0, tbl2ID1Worker1[i]);
      tbl2Worker1.putLong(1, tbl2ID2Worker1[i]);
      tbl2Worker1.putLong(1, tbl2ID1Worker1[i]);
      tbl2Worker1.putLong(0, tbl2ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl2Worker2; i++) {
      tbl2Worker2.putLong(0, tbl2ID1Worker2[i]);
      tbl2Worker2.putLong(1, tbl2ID2Worker2[i]);
      tbl2Worker2.putLong(1, tbl2ID1Worker2[i]);
      tbl2Worker2.putLong(0, tbl2ID2Worker2[i]);
    }
    TupleBatchBuffer table2 = new TupleBatchBuffer(table2Schema);
    table2.unionAll(tbl2Worker1);
    table2.unionAll(tbl2Worker2);
    createTable(
        workerIDs[0], RelationKey.of("test", "test", "g"), "node_id long, node_id_another long");
    createTable(
        workerIDs[1], RelationKey.of("test", "test", "g"), "node_id long, node_id_another long");
    while ((tb = tbl2Worker1.popAny()) != null) {
      insert(workerIDs[0], RelationKey.of("test", "test", "g"), table2Schema, tb);
    }
    while ((tb = tbl2Worker2.popAny()) != null) {
      insert(workerIDs[1], RelationKey.of("test", "test", "g"), table2Schema, tb);
    }

    // generate the correct answer in memory
    TupleBatchBuffer tmp = getConnectedComponentsResult(table2, table1Schema);
    HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(tmp);

    final ExchangePairID joinArrayId1 = ExchangePairID.newID();
    final ExchangePairID joinArrayId2 = ExchangePairID.newID();
    final ExchangePairID joinArrayId3 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpId = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpId = ExchangePairID.newID();
    final ExchangePairID serverOpId = ExchangePairID.newID();
    List<RootOperator> plan1 =
        generatePlan(
            table1Schema,
            table2Schema,
            joinArrayId1,
            joinArrayId2,
            joinArrayId3,
            eoiReceiverOpId,
            eosReceiverOpId,
            serverOpId,
            prioritized);
    final Consumer eoiReceiver =
        new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpId, workerIDs);
    final UnionAll union = new UnionAll(new Operator[] {eoiReceiver});
    final EOSController eosController =
        new EOSController(union, new ExchangePairID[] {eosReceiverOpId}, workerIDs);
    plan1.add(eosController);
    List<RootOperator> plan2 =
        generatePlan(
            table1Schema,
            table2Schema,
            joinArrayId1,
            joinArrayId2,
            joinArrayId3,
            eoiReceiverOpId,
            eosReceiverOpId,
            serverOpId,
            prioritized);

    if (failure) {
      for (RootOperator root : plan1) {
        Operator[] children = root.getChildren();
        final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, children[0], true);
        children[0] = di;
        root.setChildren(children);
      }
      for (RootOperator root : plan2) {
        Operator[] children = root.getChildren();
        final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, children[0], true);
        children[0] = di;
        root.setChildren(children);
      }
    }

    HashMap<Integer, SubQueryPlan> workerPlans = new HashMap<Integer, SubQueryPlan>();
    workerPlans.put(workerIDs[0], new SubQueryPlan(plan1.toArray(new RootOperator[plan1.size()])));
    workerPlans.put(workerIDs[1], new SubQueryPlan(plan2.toArray(new RootOperator[plan2.size()])));

    final CollectConsumer serverCollect = new CollectConsumer(table1Schema, serverOpId, workerIDs);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(queueStore));

    if (!failure) {
      server.getQueryManager().submitQuery("", "", "", serverPlan, workerPlans).get();
    } else {
      workerPlans.get(workerIDs[0]).setFTMode(FTMode.REJOIN);
      workerPlans.get(workerIDs[1]).setFTMode(FTMode.REJOIN);
      serverPlan.setFTMode(FTMode.REJOIN);

      ListenableFuture<Query> qf =
          server.getQueryManager().submitQuery("", "", "", serverPlan, workerPlans);
      Thread.sleep(1000);
      LOGGER.info("killing worker " + workerIDs[1] + "!");
      workerProcess[1].destroy();
      Query queryState = qf.get();
      assertTrue(server.getQueryManager().queryCompleted(queryState.getQueryId()));
      LOGGER.info("query {} finished.", queryState.getQueryId());
      assertEquals(Status.SUCCESS, queryState.getStatus());
    }

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

  @Test
  public void connectedComponentsNoFailureTest() throws Exception {
    connectedComponents(false, false);
  }

  @Test
  @Ignore
  public void connectedComponentsFailureTest() throws Exception {
    connectedComponents(true, false);
  }

  @Test
  public void connectedComponentsNoFailurePrioritizedTest() throws Exception {
    connectedComponents(false, true);
  }

  @Test
  @Ignore
  public void connectedComponentsFailurePrioritizedTest() throws Exception {
    connectedComponents(true, true);
  }
}
