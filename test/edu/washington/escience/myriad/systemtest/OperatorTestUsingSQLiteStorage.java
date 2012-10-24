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
import edu.washington.escience.myriad.operator.Project;
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
  public static void joinTestSQLite(final String[] args) throws DbException, IOException {
    /*
     * tested on twitter data, format: followee \t follower, in twitter_vr_slice_0_62.net. numIter controls the num of
     * iterations. currently the final output is all the reachable pairs in numIter hops. (not the union of less or
     * equal than numIter hops, so not transitive closure). there is another cpp program to help check the correctness
     * of small scale data. need to change to unit test when that branch is done.
     */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };

    final Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    final Schema tableSchema2 = tableSchema1;
    final Schema outputSchema = tableSchema1;
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);
    final int numPartition = 2;

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable1", tableSchema1);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable1.db", "select * from testtable1", tableSchema2);
    // currently need to replicate the db file because of multi-thread sqlite thing. going to check if there is a
    // smarter way later.

    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column

    final int numIter = 3;
    final ShuffleProducer sp0[] = new ShuffleProducer[numIter];
    final ShuffleProducer sp1[] = new ShuffleProducer[numIter];
    final ShuffleProducer sp2[] = new ShuffleProducer[numIter];
    final ShuffleConsumer sc0[] = new ShuffleConsumer[numIter];
    final ShuffleConsumer sc1[] = new ShuffleConsumer[numIter];
    final ShuffleConsumer sc2[] = new ShuffleConsumer[numIter];
    final LocalJoin localjoin[] = new LocalJoin[numIter];
    final Project proj[] = new Project[numIter];
    final DupElim dupelim[] = new DupElim[numIter];
    final SQLiteQueryScan scan[] = new SQLiteQueryScan[numIter];
    ExchangePairID arrayID1, arrayID2, arrayID0;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    sp1[0] = new ShuffleProducer(scan1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
    sp2[0] = new ShuffleProducer(scan2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);

    for (int i = 1; i < numIter; ++i) {
      sc1[i] = new ShuffleConsumer(sp1[i - 1], arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] });
      sc2[i] = new ShuffleConsumer(sp2[i - 1], arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] });
      localjoin[i] = new LocalJoin(joinSchema, sc1[i], sc2[i], new int[] { 1 }, new int[] { 0 });
      proj[i] = new Project(new Integer[] { 0, 3 }, localjoin[i]);
      arrayID0 = ExchangePairID.newID();
      sp0[i] = new ShuffleProducer(proj[i], arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
      sc0[i] = new ShuffleConsumer(sp0[i], arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] });
      dupelim[i] = new DupElim(sc0[i]);
      scan[i] = new SQLiteQueryScan("testtable" + (i + 1) + ".db", "select * from testtable1", tableSchema1);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();
      sp1[i] = new ShuffleProducer(scan[i], arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
      sp2[i] = new ShuffleProducer(dupelim[i], arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    }
    final CollectProducer cp = new CollectProducer(dupelim[numIter - 1], serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp);
    workerPlans.put(WORKER_ID[1], cp);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    // Server.runningInstance.exchangeSchema.put(serverReceiveID, outputSchema);
    // final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    // serverPlan.setInputBuffer(buffer);
    // Server.runningInstance.dataBuffer.put(serverPlan.getOperatorID(), buffer);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(0, serverPlan);
  }

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
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by id

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
