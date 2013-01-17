package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.TupleSource;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;

public class SplitDataTest extends SystemTestBase {

  // @Test
  public void splitDataTest() throws DbException, IOException, CatalogException {
    /* Create a source of tuples containing the numbers 1 to 10001. */
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    for (long i = 0; i < 10001; ++i) {
      tuples.put(0, i);
      tuples.put(1, "row" + i);
    }
    final TupleSource source = new TupleSource(tuples);

    /* Create the shuffle producer. */
    final ExchangePairID shuffleId = ExchangePairID.newID();
    final ShuffleProducer scatter =
        new ShuffleProducer(source, shuffleId, WORKER_ID, new RoundRobinPartitionFunction(WORKER_ID.length));
    /* ... and the corresponding shuffle consumer. */
    final ShuffleConsumer gather = new ShuffleConsumer(schema, shuffleId, new int[] { MASTER_ID });

    /* Create the Insert operator */
    final SQLiteInsert insert = new SQLiteInsert(gather, "tuples_rr", null, null, true);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    for (final int i : WORKER_ID) {
      workerPlans.put(i, new Operator[] { insert });
    }

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    scatter.setConnectionPool(Server.runningInstance.getConnectionPool());

    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    while (Server.runningInstance.startServerQuery(0, scatter) != true) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }
}
