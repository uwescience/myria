package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalCountingJoin;
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

public class OperatorTestUsingSQLiteStorage extends SystemTestBase {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OperatorTestUsingSQLiteStorage.class);

  @Test
  public void dupElimTest() throws Exception {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], testtableKey, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableKey, "id long, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedResults = TestUtils.distinct(tbb);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
      insert(WORKER_ID[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by id

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(testtableKey, schema);

    final DupElim dupElimOnScan = new DupElim(scanTable);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, collectID, WORKER_ID[0]);
    final CollectConsumer cc1 = new CollectConsumer(cp1.getSchema(), collectID, WORKER_ID);
    final DupElim dumElim3 = new DupElim(cc1);
    workerPlans
        .put(WORKER_ID[0], new RootOperator[] { cp1, new CollectProducer(dumElim3, serverReceiveID, MASTER_ID) });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0] });
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
    TestUtils.assertTupleBagEqual(expectedResults, resultBag);

  }

  @Test
  public void dupElimTestSingleWorker() throws Exception {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], testtableKey, "id long, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedResults = TestUtils.distinct(tbb);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by id

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(testtableKey, schema);

    final DupElim dupElimOnScan = new DupElim(scanTable);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, serverReceiveID, MASTER_ID);
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0] });
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
    TestUtils.assertTupleBagEqual(expectedResults, resultBag);

  }

  @Test
  public void joinTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final ShuffleProducer sp1 =
        new ShuffleProducer(scan1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalJoin localjoin = new LocalJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
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

  @Test
  public void simpleJoinTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final HashMap<Tuple, Integer> expectedResult = simpleFixedJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final ShuffleProducer sp1 =
        new ShuffleProducer(scan1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalJoin localjoin = new LocalJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
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

  @Test
  public void countingJoinTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();
    int expectedCount = 0;
    for (Integer t : expectedResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final ShuffleProducer sp1 =
        new ShuffleProducer(scan1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalCountingJoin localjoin = new LocalCountingJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).sync();

    TupleBatch tb = null;
    int actual = 0;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        actual += tb.getInt(0, 0);
      }
    }
    assertTrue(actual == expectedCount);
  }
}
