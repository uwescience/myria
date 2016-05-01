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
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Applys;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
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
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class TransitiveClosureWithEOITest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 400;
  private final int numTbl1Worker1 = 50;
  private final int numTbl1Worker2 = 60;

  public boolean[][] allNodeTransitiveClosure(final TupleBatchBuffer table1, final Schema schema) {
    // a brute force check

    final Iterator<List<? extends Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      List<? extends Column<?>> output = tbs.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).getObject(i).toString());
        int fe = Integer.parseInt(output.get(1).getObject(i).toString());
        graph[fr][fe] = true;
      }
    }

    while (true) {
      boolean update = false;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (graph[j][i] && graph[i][k] && !graph[j][k]) {
              graph[j][k] = true;
              update = true;
            }
          }
        }
      }
      if (!update) {
        break;
      }
    }
    return graph;

  }

  @Test
  public void transitiveClosure() throws Exception {
    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    // generate the graph
    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.putLong(0, tbl1ID1Worker1[i]);
      tbl1Worker1.putLong(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.putLong(0, tbl1ID1Worker2[i]);
      tbl1Worker2.putLong(1, tbl1ID2Worker2[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);

    createTable(workerIDs[0], testtableKey, "follower long, followee long");
    createTable(workerIDs[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], testtableKey, tableSchema, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(workerIDs[1], testtableKey, tableSchema, tb);
    }

    boolean[][] graph = allNodeTransitiveClosure(table1, tableSchema);
    // generate the correct answer in memory
    TupleBatchBuffer expectedTBB = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < MaxID; ++i) {
      for (int j = 0; j < MaxID; ++j) {
        if (graph[i][j]) {
          expectedTBB.putLong(0, i);
          expectedTBB.putLong(1, j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // parallel query generation, duplicate db files
    final DbQueryScan scan1 = new DbQueryScan(testtableKey, tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(testtableKey, tableSchema);

    final int numPartition = 2;
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);

    ExchangePairID joinArray1ID = ExchangePairID.newID();

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, joinArray1ID, new int[] { workerIDs[0], workerIDs[1] }, pf1);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(tableSchema, joinArray1ID, new int[] { workerIDs[0], workerIDs[1] });

    ExchangePairID beforeIngress1 = ExchangePairID.newID();
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, beforeIngress1, new int[] { workerIDs[0], workerIDs[1] }, pf0);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(tableSchema, beforeIngress1, new int[] { workerIDs[0], workerIDs[1] });

    ExchangePairID beforeIngress2 = ExchangePairID.newID();
    final GenericShuffleProducer sp3_worker1 =
        new GenericShuffleProducer(null, beforeIngress2, new int[] { workerIDs[0], workerIDs[1] }, pf0);
    final GenericShuffleProducer sp3_worker2 =
        new GenericShuffleProducer(null, beforeIngress2, new int[] { workerIDs[0], workerIDs[1] }, pf0);
    // set their children later
    final GenericShuffleConsumer sc3_worker1 =
        new GenericShuffleConsumer(tableSchema, beforeIngress2, new int[] { workerIDs[0], workerIDs[1] });
    final GenericShuffleConsumer sc3_worker2 =
        new GenericShuffleConsumer(tableSchema, beforeIngress2, new int[] { workerIDs[0], workerIDs[1] });

    final ExchangePairID eosReceiverOpID = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpID = ExchangePairID.newID();
    final Consumer eosReceiver = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpID, new int[] { workerIDs[0] });

    final IDBController idbController_worker1 =
        new IDBController(0, eoiReceiverOpID, workerIDs[0], sc2, sc3_worker1, eosReceiver, new DupElim());
    final IDBController idbController_worker2 =
        new IDBController(0, eoiReceiverOpID, workerIDs[0], sc2, sc3_worker2, eosReceiver, new DupElim());

    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayProducer multiProducer_worker1 =

    new LocalMultiwayProducer(idbController_worker1, new ExchangePairID[] { consumerID1, consumerID2 });
    final LocalMultiwayProducer multiProducer_worker2 =
        new LocalMultiwayProducer(idbController_worker2, new ExchangePairID[] { consumerID1, consumerID2 });
    final LocalMultiwayConsumer send2join_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2join_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2server_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final LocalMultiwayConsumer send2server_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID2);

    final SymmetricHashJoin join_worker1 =
        new SymmetricHashJoin(sc1, send2join_worker1, new int[] { 1 }, new int[] { 0 }, new int[] { 0 },
            new int[] { 1 });
    final SymmetricHashJoin join_worker2 =
        new SymmetricHashJoin(sc1, send2join_worker2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 },
            new int[] { 1 });
    sp3_worker1.setChildren(new Operator[] { join_worker1 });
    sp3_worker2.setChildren(new Operator[] { join_worker2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp_worker1 = new CollectProducer(send2server_worker1, serverReceiveID, MASTER_ID);
    final CollectProducer cp_worker2 = new CollectProducer(send2server_worker2, serverReceiveID, MASTER_ID);

    final Consumer eoiReceiver = new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpID, workerIDs);
    final UnionAll unionAll = new UnionAll(new Operator[] { eoiReceiver });
    final EOSController eosController =
        new EOSController(unionAll, new ExchangePairID[] { eosReceiverOpID }, workerIDs);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] {
        cp_worker1, multiProducer_worker1, sp1, sp2, sp3_worker1, eosController });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp_worker2, multiProducer_worker2, sp1, sp2, sp3_worker2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final EmptySink serverPlan = new EmptySink(queueStore);

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

  @Test
  public void simpleSingleWorkerTransitiveClosure() throws Exception {
    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    // generate the graph
    long[] tbl1ID1Worker1 = new long[] { 0, 1, 2, 2 };
    long[] tbl1ID2Worker1 = new long[] { 1, 2, 3, 4 };
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < tbl1ID1Worker1.length; i++) {
      table1.putLong(0, tbl1ID1Worker1[i]);
      table1.putLong(1, tbl1ID2Worker1[i]);
    }

    TupleBatchBuffer seed = new TupleBatchBuffer(tableSchema);
    seed.putLong(0, 0l);
    seed.putLong(1, 0l);
    RelationKey seedKey = RelationKey.of("test", "test", "identity");
    RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], seedKey, "follower long, followee long");
    insert(workerIDs[0], seedKey, tableSchema, seed.popAny());

    createTable(workerIDs[0], testtableKey, "follower long, followee long");
    for (TupleBatch tb : table1.getAll()) {
      insert(workerIDs[0], testtableKey, tableSchema, tb);
    }

    boolean[][] graph = allNodeTransitiveClosure(table1, tableSchema);
    // transitive closure from 0
    TupleBatchBuffer expectedTBB = new TupleBatchBuffer(tableSchema);
    for (int j = 0; j < MaxID; ++j) {
      if (graph[0][j]) {
        expectedTBB.putLong(0, 0l);
        expectedTBB.putLong(1, j);
        LOGGER.debug(0 + "\t" + j);
      }
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // parallel query generation, duplicate db files
    final DbQueryScan scan1 = new DbQueryScan(testtableKey, tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(seedKey, tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpID = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID = ExchangePairID.newID();
    final LocalMultiwayConsumer sendBack = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2server = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final Consumer eosReceiver = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpID, new int[] { workerIDs[0] });

    final IDBController idbController =
        new IDBController(0, eoiReceiverOpID, workerIDs[0], scan2, sendBack, eosReceiver, new DupElim());

    final Consumer eoiReceiver =
        new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpID, new int[] { workerIDs[0] });
    final UnionAll unionAll = new UnionAll(new Operator[] { eoiReceiver });
    final EOSController eosController =
        new EOSController(unionAll, new ExchangePairID[] { eosReceiverOpID }, new int[] { workerIDs[0] });

    final int numPartition = 1;
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);
    ExchangePairID joinArray1ID, joinArray2ID;
    joinArray1ID = ExchangePairID.newID();
    joinArray2ID = ExchangePairID.newID();

    final GenericShuffleProducer sp1 = new GenericShuffleProducer(scan1, joinArray1ID, new int[] { workerIDs[0] }, pf1);
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(idbController, joinArray2ID, new int[] { workerIDs[0] }, pf0);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), joinArray1ID, new int[] { workerIDs[0] });
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), joinArray2ID, new int[] { workerIDs[0] });

    final List<String> joinOutputColumns = ImmutableList.of("follower1", "followee1", "follower2", "followee2");
    final SymmetricHashJoin join = new SymmetricHashJoin(joinOutputColumns, sc1, sc2, new int[] { 0 }, new int[] { 1 });
    final Apply proj = Applys.columnSelect(join, 2, 1);
    ExchangePairID beforeDE = ExchangePairID.newID();

    final GenericShuffleProducer sp3 = new GenericShuffleProducer(proj, beforeDE, new int[] { workerIDs[0] }, pf0);
    final GenericShuffleConsumer sc3 =
        new GenericShuffleConsumer(sp3.getSchema(), beforeDE, new int[] { workerIDs[0] });
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(sc3, new DupElim());
    final LocalMultiwayProducer multiProducer =
        new LocalMultiwayProducer(dupelim, new ExchangePairID[] { consumerID1, consumerID2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(send2server, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp, multiProducer, sp1, sp2, sp3, eosController });

    final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverReceiveID, new int[] { workerIDs[0] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final EmptySink serverPlan = new EmptySink(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
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
