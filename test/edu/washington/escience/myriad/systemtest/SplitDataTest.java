package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.operator.TupleSource;
import edu.washington.escience.myriad.operator.agg.Aggregate;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;

public class SplitDataTest extends SystemTestBase {

  @Test
  public void splitDataTest() throws Exception {
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

    /* ... and the corresponding shuffle consumer. */
    final ShuffleConsumer gather = new ShuffleConsumer(schema, shuffleId, new int[] { MASTER_ID });

    final RelationKey tuplesRRKey = RelationKey.of("test", "test", "tuples_rr");

    /* Create the Insert operator */
    final SQLiteInsert insert = new SQLiteInsert(gather, tuplesRRKey, true);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    for (final int i : WORKER_ID) {
      workerPlans.put(i, new RootOperator[] { insert });
    }

    server.submitQueryPlan(scatter, workerPlans).sync();

    /*** TEST PHASE 2: Count them up, make sure the answer agrees. ***/
    /* Create the worker plan: QueryScan with count, then send it to master. */
    Schema countResultSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("localCount"));
    final SQLiteQueryScan scanCount =
        new SQLiteQueryScan("SELECT COUNT(*) FROM " + tuplesRRKey.toString("sqlite"), countResultSchema);

    final ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer send = new CollectProducer(scanCount, collectId, 0);
    workerPlans.clear();
    for (final int i : WORKER_ID) {
      workerPlans.put(i, new RootOperator[] { send });
    }
    /* Create the Server plan: CollectConsumer and Sum. */
    final CollectConsumer receive = new CollectConsumer(countResultSchema, collectId, WORKER_ID);
    Aggregate sumCount =
        new Aggregate(receive, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_COUNT });
    final LinkedBlockingQueue<TupleBatch> aggResult = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(aggResult, sumCount);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    /* Actually dispatch the worker plans. */
    /* Start the query and collect the results. */
    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatch result = aggResult.take();

    /* Sanity-check the results, sum them, then confirm. */
    assertTrue(result.getLong(0, 0) == WORKER_ID.length);

    LOGGER.debug("numTuplesInsert=" + numTuplesInserted + ", sum=" + result.getObject(0, 0));
    assertTrue(result.getLong(1, 0) == numTuplesInserted);

  }
}
