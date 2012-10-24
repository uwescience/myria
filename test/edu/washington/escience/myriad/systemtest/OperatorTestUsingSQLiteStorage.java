package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
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

public class OperatorTestUsingSQLiteStorage extends SystemTestBase {

  @Test
  public void dupElimTest() throws DbException, IOException {
    String testtableName = "testtable";
    createTable(WORKER_ID[0], testtableName, testtableName, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableName, testtableName, "id long, name varchar(20)");

    String[] names = randomFixedLengthNumericString(1000, 1005, 200, 20);
    long[] ids = randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    HashSet<Tuple> expectedResults = distinct(tbb);

    _TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableName, schema, tb);
      insert(WORKER_ID[1], testtableName, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scanTable =
        new SQLiteQueryScan(testtableName + ".db", "select * from " + testtableName, schema);

    final DupElim dupElimOnScan = new DupElim(scanTable);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    final CollectProducer cp1 = new CollectProducer(dupElimOnScan, collectID, WORKER_ID[0]);
    final CollectConsumer cc1 = new CollectConsumer(cp1, collectID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final DupElim dumElim3 = new DupElim(cc1);
    workerPlans.put(WORKER_ID[0], new CollectProducer(dumElim3, serverReceiveID, MASTER_ID));
    workerPlans.put(WORKER_ID[1], cp1);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0] });
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    TupleBatchBuffer result = null;
    while ((result = Server.runningInstance.startServerQuery(0, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    HashSet<Tuple> resultSet = tupleBatchToTupleSet(result);

    assertTrue(resultSet.size() == expectedResults.size());
    for (Tuple t : resultSet) {
      assertTrue(expectedResults.contains(t));
    }
  }

  @Test
  public void joinTest() throws DbException, IOException {

    HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final SQLiteQueryScan scan1 =
        new SQLiteQueryScan(JOIN_TEST_TABLE_1 + ".db", "select * from " + JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final SQLiteQueryScan scan2 =
        new SQLiteQueryScan(JOIN_TEST_TABLE_2 + ".db", "select * from " + JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final ShuffleProducer sp1 =
        new ShuffleProducer(scan1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1, table1ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2, table2ShuffleID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalJoin localjoin = new LocalJoin(outputSchema, sc1, sc2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp1);
    workerPlans.put(WORKER_ID[1], cp1);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    TupleBatchBuffer result = null;
    while ((result = Server.runningInstance.startServerQuery(0, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    HashMap<Tuple, Integer> resultBag = tupleBatchToTupleBag(result);

    assertTrue(resultBag.size() == expectedResult.size());
    for (Entry<Tuple, Integer> e : resultBag.entrySet()) {
      assertTrue(expectedResult.get(e.getKey()).equals(e.getValue()));
    }

  }

}
