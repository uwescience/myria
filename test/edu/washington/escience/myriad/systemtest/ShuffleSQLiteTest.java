package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.BlockingDataReceiver;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.SQLiteTupleBatch;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class ShuffleSQLiteTest extends SystemTestBase {

  @Test
  public void shuffleTestSQLite() throws DbException, IOException {

    final Type[] types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] columnNames = new String[] { "id", "name" };
    final Schema schema = new Schema(types, columnNames);

    HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();
    String temptable1Name = "temptable1";
    String temptable2Name = "temptable2";

    createTable(WORKER_ID[0], "temptable", temptable1Name, "id int, name varchar(20)");
    createTable(WORKER_ID[0], "temptable", temptable2Name, "id int, name varchar(20)");
    createTable(WORKER_ID[1], "temptable", temptable1Name, "id int, name varchar(20)");
    createTable(WORKER_ID[1], "temptable", temptable2Name, "id int, name varchar(20)");

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID shuffle1ID = ExchangePairID.newID();
    final ExchangePairID shuffle2ID = ExchangePairID.newID();

    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable1.db", "select * from testtable1", schema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable2.db", "select * from testtable2", schema);
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);

    final ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);

    final SQLiteTupleBatch bufferWorker1 = new SQLiteTupleBatch(schema, "temptable.db", "temptable1");
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1, shuffle1ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingDataReceiver buffer1 = new BlockingDataReceiver(bufferWorker1, sc1);

    final SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(schema, "temptable.db", "temptable2");
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2, shuffle2ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingDataReceiver buffer2 = new BlockingDataReceiver(bufferWorker2, sc2);

    final SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("temptable.db",
            "select * from temptable1 inner join temptable2 on temptable1.name=temptable2.name", outputSchema,
            new Operator[] { buffer1, buffer2 });

    final CollectProducer cp = new CollectProducer(ssp, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp);
    workerPlans.put(WORKER_ID[1], cp);

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
