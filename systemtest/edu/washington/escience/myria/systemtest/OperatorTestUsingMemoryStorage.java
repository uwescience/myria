package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.memorydb.MemoryStoreInfo;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.MergeJoin;
import edu.washington.escience.myria.operator.RightHashCountingJoin;
import edu.washington.escience.myria.operator.RightHashJoin;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashCountingJoin;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class OperatorTestUsingMemoryStorage extends SystemTestBase {
  /** The logger for this class. */
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OperatorTestUsingMemoryStorage.class);

  final RelationKey singleTableKey = RelationKey.of("test", "test", "single_table");
  final Schema singleTableSchema = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of(
      "id", "name"));
  final TupleBatchBuffer singleTableData;
  {
    final String[] namesSingleTable = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] idsSingleTable = TestUtils.randomLong(1000, 1005, namesSingleTable.length);

    singleTableData = new TupleBatchBuffer(singleTableSchema);
    for (int i = 0; i < namesSingleTable.length; i++) {
      singleTableData.putLong(0, idsSingleTable[i]);
      singleTableData.putString(1, namesSingleTable[i]);
    }
  }

  final RelationKey randomJoinTable1Key = RelationKey.of("test", "test", "random_join_table1");
  final RelationKey randomJoinTable2Key = RelationKey.of("test", "test", "random_join_table2");

  final TupleBatchBuffer randomJoinTable1Data;
  final TupleBatchBuffer randomJoinTable2Data;
  final Map<Tuple, Integer> expectedRandomJoinResult;
  {
    TupleBatchBuffer[] data = null;
    try {
      data = super.simpleRandomJoinTestBaseData();
    } catch (Exception ee) {
      throw new RuntimeException(ee);
    }
    randomJoinTable1Data = data[4];
    randomJoinTable2Data = data[5];
    expectedRandomJoinResult =
        ImmutableMap.<Tuple, Integer> copyOf(TestUtils.naturalJoin(randomJoinTable1Data, randomJoinTable2Data, 0, 0));
  }

  final RelationKey fixedJoinTable1Key = RelationKey.of("test", "test", "fixed_join_table1");
  final RelationKey fixedJoinTable2Key = RelationKey.of("test", "test", "fixed_join_table2");
  final TupleBatchBuffer fixedJoinTable1Data;
  final TupleBatchBuffer fixedJoinTable2Data;
  final Map<Tuple, Integer> expectedFixedJoinResult;
  {
    TupleBatchBuffer[] data = null;
    try {
      data = super.simpleFixedJoinTestBaseData();
    } catch (Exception ee) {
      throw new RuntimeException(ee);
    }
    fixedJoinTable1Data = data[4];
    fixedJoinTable2Data = data[5];
    expectedFixedJoinResult =
        ImmutableMap.<Tuple, Integer> copyOf(TestUtils.naturalJoin(fixedJoinTable1Data, fixedJoinTable2Data, 0, 0));
  }

  final ImmutableList<Type> joinOutputTypes = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE,
      Type.STRING_TYPE);
  final ImmutableList<String> joinOutputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
  final Schema joinOutputSchema = new Schema(joinOutputTypes, joinOutputColumnNames);

  final MemoryStoreInfo memoryStoreInfo = new MemoryStoreInfo();

  private volatile boolean inserted = false;

  @Before
  public void prepareData() throws InterruptedException, DbException {
    if (!inserted) {
      inserted = true;
      server.ingestDatasetMemory(singleTableKey, ImmutableSet.<Integer> of(workerIDs[0], workerIDs[1]), null,
          new TupleSource(singleTableData.getAll())).awaitUninterruptibly();
      server.ingestDatasetMemory(randomJoinTable1Key, ImmutableSet.<Integer> of(workerIDs[0], workerIDs[1]), null,
          new TupleSource(randomJoinTable1Data.getAll())).awaitUninterruptibly();
      server.ingestDatasetMemory(randomJoinTable2Key, ImmutableSet.<Integer> of(workerIDs[0], workerIDs[1]), null,
          new TupleSource(randomJoinTable2Data.getAll())).awaitUninterruptibly();
      server.ingestDatasetMemory(fixedJoinTable1Key, ImmutableSet.<Integer> of(workerIDs[0], workerIDs[1]), null,
          new TupleSource(fixedJoinTable1Data.getAll())).awaitUninterruptibly();
      server.ingestDatasetMemory(fixedJoinTable2Key, ImmutableSet.<Integer> of(workerIDs[0], workerIDs[1]), null,
          new TupleSource(fixedJoinTable2Data.getAll())).awaitUninterruptibly();
    }
  }

  @Test
  public void dupElimTest() throws Exception {
    final Map<Tuple, Integer> expectedResults = TestUtils.distinct(singleTableData);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(memoryStoreInfo, singleTableKey, singleTableSchema);

    final StreamingStateWrapper dupElimOnScan = new StreamingStateWrapper(scanTable, new DupElim());
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, collectID, workerIDs[0]);
    final CollectConsumer cc1 = new CollectConsumer(cp1.getSchema(), collectID, workerIDs);
    final StreamingStateWrapper dumElim3 = new StreamingStateWrapper(cc1, new DupElim());
    workerPlans
        .put(workerIDs[0], new RootOperator[] { cp1, new CollectProducer(dumElim3, serverReceiveID, MASTER_ID) });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(singleTableSchema, serverReceiveID, new int[] { workerIDs[0] });
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
    TestUtils.assertTupleBagEqual(expectedResults, resultBag);
  }

  @Test
  public void joinTest() throws Exception {

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, randomJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, randomJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final SymmetricHashJoin localjoin =
        new SymmetricHashJoin(joinOutputColumnNames, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(joinOutputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
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
    TestUtils.assertTupleBagEqual(expectedRandomJoinResult, resultBag);

  }

  @Test
  public void rightHashJoinTest() throws Exception {

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, randomJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, randomJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final RightHashJoin localjoin =
        new RightHashJoin(joinOutputColumnNames, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(joinOutputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
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
    TestUtils.assertTupleBagEqual(expectedRandomJoinResult, resultBag);
  }

  @Test
  public void simpleJoinTest() throws Exception {

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, fixedJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, fixedJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final List<String> joinOutputColumnNames = ImmutableList.of("id_1", "name_1", "id_2", "name_2");
    final SymmetricHashJoin localjoin =
        new SymmetricHashJoin(joinOutputColumnNames, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
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
    TestUtils.assertTupleBagEqual(expectedFixedJoinResult, resultBag);

  }

  @Test
  public void simpleRightHashJoinTest() throws Exception {

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, fixedJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, fixedJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final List<String> joinOutputColumnNames = ImmutableList.of("id_1", "name_1", "id_2", "name_2");
    final RightHashJoin localjoin =
        new RightHashJoin(joinOutputColumnNames, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
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
    TestUtils.assertTupleBagEqual(expectedFixedJoinResult, resultBag);

  }

  @Test
  public void countingJoinTest() throws Exception {

    int expectedCount = 0;
    for (Integer t : expectedRandomJoinResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, randomJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, randomJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =

    new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final SymmetricHashCountingJoin localjoin =
        new SymmetricHashCountingJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).sync();

    TupleBatch tb = null;
    long actual = 0;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        actual += tb.getLong(0, 0);
      }
    }
    assertEquals(expectedCount, actual);
  }

  @Test
  public void unbalancedCountingJoinTest() throws Exception {

    int expectedCount = 0;
    for (Integer t : expectedRandomJoinResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, randomJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, randomJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =

    new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final RightHashCountingJoin localjoin = new RightHashCountingJoin(sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).sync();

    TupleBatch tb = null;
    long actual = 0;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        actual += tb.getLong(0, 0);
      }
    }
    assertEquals(expectedCount, actual);
  }

  @Test
  public void mergeJoinTest() throws Exception {

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(memoryStoreInfo, fixedJoinTable1Key, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(memoryStoreInfo, fixedJoinTable2Key, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final InMemoryOrderBy order1 = new InMemoryOrderBy(sc1, new int[] { 0 }, new boolean[] { true });

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final InMemoryOrderBy order2 = new InMemoryOrderBy(sc2, new int[] { 0 }, new boolean[] { true });

    final List<String> joinOutputColumnNames = ImmutableList.of("id_1", "name_1", "id_2", "name_2");
    final MergeJoin localjoin =
        new MergeJoin(joinOutputColumnNames, order1, order2, new int[] { 0 }, new int[] { 0 }, new boolean[] { true });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
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
    TestUtils.assertTupleBagEqual(expectedFixedJoinResult, resultBag);
  }

}
