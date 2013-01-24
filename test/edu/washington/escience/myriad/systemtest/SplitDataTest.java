package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.TupleSource;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;

public class SplitDataTest extends SystemTestBase {

  @Test
  public void splitDataTest() throws DbException, IOException, CatalogException {
    /* Create a source of tuples containing the numbers 1 to 10001. */
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    final int numTuplesInserted = 10001;
    for (long i = 0; i < numTuplesInserted; ++i) {
      tuples.put(0, i);
      tuples.put(1, "row" + i);
    }
    final TupleSource source = new TupleSource(tuples);

    /*** TEST PHASE 1: Insert all the tuples. ***/
    /* Create the shuffle producer. */
    final ExchangePairID shuffleId = ExchangePairID.newID();
    final ShuffleProducer scatter =
        new ShuffleProducer(source, shuffleId, WORKER_ID, new RoundRobinPartitionFunction(WORKER_ID.length));
    scatter.setConnectionPool(server.getConnectionPool());
    /* ... and the corresponding shuffle consumer. */
    final ShuffleConsumer gather = new ShuffleConsumer(schema, shuffleId, new int[] { MASTER_ID });

    /* Create the Insert operator */
    final SQLiteInsert insert = new SQLiteInsert(gather, "tuples_rr", null, null, true);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    for (final int i : WORKER_ID) {
      workerPlans.put(i, new Operator[] { insert });
    }

    long queryId = 7L;

    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query part 1 (round robin ruple shuffle) dispatched to the workers");
    while (server.startServerQuery(queryId, scatter) != true) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    while (!server.queryCompleted(queryId)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    /*** TEST PHASE 2: Count them up, make sure the answer agrees. ***/
    /* Create the worker plan: QueryScan with count, then send it to master. */
    Schema countResultSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("localCount"));
    final SQLiteQueryScan scanCount = new SQLiteQueryScan(null, "SELECT COUNT(*) FROM tuples_rr", countResultSchema);
    final ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer send = new CollectProducer(scanCount, collectId, 0);
    workerPlans.clear();
    for (final int i : WORKER_ID) {
      workerPlans.put(i, new Operator[] { send });
    }
    /* Create the Server plan: CollectConsumer and Sum. */
    final CollectConsumer receive = new CollectConsumer(countResultSchema, collectId, WORKER_ID);

    /* Actually dispatch the worker plans. */
    queryId = 8L;
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query part 2 (count, collect, sum) dispatched to the workers");
    /* Start the query and collect the results. */
    queryId = 8L;
    TupleBatchBuffer result = server.startServerQuery(queryId, receive);
    while (result == null) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
      result = server.startServerQuery(queryId, receive);
    }
    /* Sanity-check the results, sum them, then confirm. */
    TupleBatch aggregate = result.popAny();
    assertTrue(aggregate.numTuples() == WORKER_ID.length);
    assertTrue(aggregate.getSchema().numFields() == 1);
    long sum = 0;
    for (int i = 0; i < aggregate.numTuples(); ++i) {
      sum += aggregate.getLong(0, i);
    }
    LOGGER.debug("numTuplesInsert=" + numTuplesInserted + ", sum=" + sum);
    assertTrue(numTuplesInserted == sum);
  }
}
