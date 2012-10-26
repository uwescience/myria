package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.table._TupleBatch;

public class CollectTest extends SystemTestBase {

  @Test
  public void collectTest() throws DbException, IOException {
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

    TupleBatchBuffer resultTBB = new TupleBatchBuffer(schema);
    resultTBB.merge(tbb);
    resultTBB.merge(tbb);
    HashMap<Tuple, Integer> expectedResults = SystemTestBase.tupleBatchToTupleBag(resultTBB);

    _TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableName, schema, tb);
      insert(WORKER_ID[1], testtableName, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final SQLiteQueryScan scanTable =
        new SQLiteQueryScan(testtableName + ".db", "select * from " + testtableName, schema);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(WORKER_ID[0], cp1);
    workerPlans.put(WORKER_ID[1], cp1);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
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

    HashMap<Tuple, Integer> resultSet = tupleBatchToTupleBag(result);

    SystemTestBase.assertTupleBagEqual(expectedResults, resultSet);

  }
}
