package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class DupElimSQLLiteTest extends SystemTestBase {

  @Test
  public void dupElimTestSQLite() throws DbException, IOException {
    String testtableName = "testtable";
    createTable(WORKER_ID[0], testtableName, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableName, "id long, name varchar(20)");

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

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
    while (!Server.runningInstance.startServerQuery(0, serverPlan)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }
}
