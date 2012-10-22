package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
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

public class ShuffleSQLLiteTest extends SystemTestBase {

  public static void shuffleTestSQLite(final String[] args) throws DbException, IOException {
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID shuffle1ID = ExchangePairID.newID();
    final ExchangePairID shuffle2ID = ExchangePairID.newID();

    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scan1 =
        new SQLiteQueryScan("testtable.db", "select * from " + inputTableName[0], inputTableSchema[0]);
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);

    final SQLiteQueryScan scan2 =
        new SQLiteQueryScan("testtable.db", "select * from " + inputTableName[1], inputTableSchema[1]);
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf);

    final SQLiteTupleBatch bufferWorker1 = new SQLiteTupleBatch(inputTableSchema[0], "temptable.db", "temptable1");
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1, shuffle1ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingDataReceiver buffer1 = new BlockingDataReceiver(bufferWorker1, sc1);

    final SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(inputTableSchema[1], "temptable.db", "temptable2");
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2, shuffle2ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingDataReceiver buffer2 = new BlockingDataReceiver(bufferWorker2, sc2);

    final SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("temptable.db",
            "select * from temptable1 inner join temptable2 on temptable1.name=temptable2.name", outputSchema,
            new Operator[] { buffer1, buffer2 });

    // DoNothingOperator dno = new DoNothingOperator(outputSchema, new Operator[] { buffer1, buffer2 });

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
    Server.runningInstance.startServerQuery(0, serverPlan);

  }
}
