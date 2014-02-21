package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.DatasetFormat;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.MergeJoin;
import edu.washington.escience.myria.operator.RightHashCountingJoin;
import edu.washington.escience.myria.operator.RightHashJoin;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashCountingJoin;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.MasterQueryPartition;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class OperatorTestUsingSQLiteStorage extends SystemTestBase {
  /** The logger for this class. */
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OperatorTestUsingSQLiteStorage.class);

  @Test
  public void dupElimTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleSet(simpleSingleTableTestBase(1000));
    JsonQueryBaseBuilder builder = new JsonQueryBaseBuilder().workers(workerIDs);
    QueryEncoding query =
        builder.scan(SINGLE_TEST_TABLE).dupElim().collect(workerIDs[0]).dupElim().masterCollect().export(
            DatasetFormat.TSV).build();
    QueryFuture qf = server.submitQuery(query);
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), (((MasterQueryPartition) qf.getQuery()))
            .getResultSchema())));

  }

  @Test
  public void dupElimTestSingleWorker() throws Exception {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    final HashMap<Tuple, Integer> expectedResult = TestUtils.distinct(tbb);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final StreamingStateWrapper dupElimOnScan = new StreamingStateWrapper(scanTable, new DupElim());
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();

    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { cp1 }));

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0] });

    final SingleQueryPlanWithArgs serverPlan =
        new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));

    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);

    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));
  }

  @Test
  public void joinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    JsonQueryBaseBuilder builder = new JsonQueryBaseBuilder().workers(workerIDs);
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    JsonQueryBaseBuilder scan1 = builder.scan(JOIN_TEST_TABLE_1).setName("scan1").shuffle(pf);
    JsonQueryBaseBuilder scan2 = builder.scan(JOIN_TEST_TABLE_2).setName("scan2").shuffle(pf);
    QueryEncoding query =
        scan1.hashEquiJoin(scan2, new int[] { 0 }, new int[] { 0, 1 }, new int[] { 0 }, new int[] { 0, 1 })
            .masterCollect().export(DatasetFormat.TSV).build();
    QueryFuture qf = server.submitQuery(query);

    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));
  }

  @Test
  public void rightHashJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA_1);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA_2);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final RightHashJoin localjoin = new RightHashJoin(outputColumnNames, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleQueryPlanWithArgs serverPlan =
        new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));

    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));
  }

  @Test
  public void simpleJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleFixedJoinTestBase();

    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    JsonQueryBaseBuilder builder = new JsonQueryBaseBuilder().workers(workerIDs);
    JsonQueryBaseBuilder scan1 = builder.scan(JOIN_TEST_TABLE_1).setName("scan1").shuffle(pf);
    JsonQueryBaseBuilder scan2 = builder.scan(JOIN_TEST_TABLE_2).setName("scan2").shuffle(pf);
    QueryEncoding query =
        scan1.hashEquiJoin(scan2, new int[] { 0 }, new int[] { 0, 1 }, new int[] { 0 }, new int[] { 0, 1 })
            .masterCollect().export(DatasetFormat.TSV).build();
    QueryFuture qf = server.submitQuery(query);
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));

  }

  @Test
  public void simpleRightHashJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleFixedJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA_1);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA_2);

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
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleQueryPlanWithArgs serverPlan =
        new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));

    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));
  }

  @Before
  public void init() {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
  }

  @Test
  public void countingJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();
    int expectedCount = 0;
    for (Integer t : expectedResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA_1);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA_2);

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
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleQueryPlanWithArgs serverPlan =
        new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));
    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);

    HashMap<Tuple, Integer> actualR =
        TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(((MasterQueryPartition) qf.getQuery()).getResultStream(),
            ((MasterQueryPartition) qf.getQuery()).getResultSchema()));
    long actual = 0;
    for (Tuple t : actualR.keySet()) {
      actual += (Long) t.get(0);
    }

    assertEquals(expectedCount, actual);
  }

  @Test
  public void unbalancedCountingJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();
    int expectedCount = 0;
    for (Integer t : expectedResult.values()) {
      expectedCount += t;
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA_1);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA_2);

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
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleQueryPlanWithArgs serverPlan =
        new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));
    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);

    HashMap<Tuple, Integer> actualR =
        TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(((MasterQueryPartition) qf.getQuery()).getResultStream(),
            ((MasterQueryPartition) qf.getQuery()).getResultSchema()));
    long actual = 0;
    for (Tuple t : actualR.keySet()) {
      actual += (Long) t.get(0);
    }

    assertEquals(expectedCount, actual);
  }

  @Test
  public void mergeJoinTest() throws Exception {

    final HashMap<Tuple, Integer> expectedResult = simpleFixedJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA_1);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA_2);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    // final String schema = "id long, name varchar(20)";

    final InMemoryOrderBy order1 = new InMemoryOrderBy(sc1, new int[] { 0 }, new boolean[] { true });
    /*
     * RelationKey tmp0 = RelationKey.of("test", "test", "tmp0"); createTable(workerIDs[0], tmp0, schema);
     * createTable(workerIDs[1], tmp0, schema); final DbInsert ins1 = new DbInsert(sc1, tmp0, true); final DbQueryScan
     * order1 = new DbQueryScan(tmp0, sp1.getSchema(), new int[] { 0 }, new boolean[] { true });
     */

    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final InMemoryOrderBy order2 = new InMemoryOrderBy(sc2, new int[] { 0 }, new boolean[] { true });
    /*
     * RelationKey tmp1 = RelationKey.of("test", "test", "tmp1"); createTable(workerIDs[0], tmp1, schema);
     * createTable(workerIDs[1], tmp1, schema); final DbInsert ins2 = new DbInsert(sc2, tmp1, true); final DbQueryScan
     * order2 = new DbQueryScan(tmp1, sp1.getSchema(), new int[] { 0 }, new boolean[] { true });
     */

    final List<String> joinOutputColumnNames = ImmutableList.of("id_1", "name_1", "id_2", "name_2");
    final MergeJoin localjoin =
        new MergeJoin(joinOutputColumnNames, order1, order2, new int[] { 0 }, new int[] { 0 }, new boolean[] { true });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new DataOutput(serverCollect, DatasetFormat.TSV));

    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(
        ((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf.getQuery())
            .getResultSchema())));
  }
}
