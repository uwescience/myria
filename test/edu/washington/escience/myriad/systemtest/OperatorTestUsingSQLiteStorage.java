package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class OperatorTestUsingSQLiteStorage extends SystemTestBase {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory
      .getLogger(OperatorTestUsingSQLiteStorage.class.getName());

  @Test
  public void dupElimTest() throws DbException, IOException, CatalogException {
    final String testtableName = "testtable";
    createTable(WORKER_ID[0], testtableName, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableName, "id long, name varchar(20)");

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
      insert(WORKER_ID[0], testtableName, schema, tb);
      insert(WORKER_ID[1], testtableName, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by id

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(null, "select * from " + testtableName, schema);

    final DupElim dupElimOnScan = new DupElim(scanTable);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, collectID, WORKER_ID[0]);
    final CollectConsumer cc1 = new CollectConsumer(cp1.getSchema(), collectID, WORKER_ID);
    final DupElim dumElim3 = new DupElim(cc1);
    workerPlans.put(WORKER_ID[0], new Operator[] { cp1, new CollectProducer(dumElim3, serverReceiveID, MASTER_ID) });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp1 });

    final long queryId = 2;

    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0] });
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);

    final HashMap<Tuple, Integer> resultSet = TestUtils.tupleBatchToTupleSet(result);

    TestUtils.assertTupleBagEqual(expectedResults, resultSet);

  }

  @Test
  public void dupElimTestSingleWorker() throws DbException, IOException, CatalogException {
    final String testtableName = "testtable";
    createTable(WORKER_ID[0], testtableName, "id long, name varchar(20)");

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
      insert(WORKER_ID[0], testtableName, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by id

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(null, "select * from " + testtableName, schema);

    final DupElim dupElimOnScan = new DupElim(scanTable);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();

    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, serverReceiveID, MASTER_ID);
    workerPlans.put(WORKER_ID[0], new Operator[] { cp1 });

    final long queryId = 0;

    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0] });
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);

    final HashMap<Tuple, Integer> resultSet = TestUtils.tupleBatchToTupleSet(result);

    TestUtils.assertTupleBagEqual(expectedResults, resultSet);

  }

  @Test
  public void joinTest() throws DbException, IOException, CatalogException {
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
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id", "name", "id", "name");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from " + JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from " + JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final ShuffleProducer sp1 =
        new ShuffleProducer(scan1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalJoin localjoin = new LocalJoin(outputSchema, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { sp1, sp2, cp1 });
    workerPlans.put(WORKER_ID[1], new Operator[] { sp1, sp2, cp1 });

    final long queryId = 1;
    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);

    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(result);

    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

  }

}
