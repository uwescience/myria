package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.DatasetFormat;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.MasterQueryPartition;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.TestUtils;

public class SplitDataTest extends SystemTestBase {

  @Test
  public void splitDataTest() throws Exception {
    /* Create a source of tuples containing the numbers 1 to 10001. */
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    final int numTuplesInserted = 10001;
    for (long i = 0; i < numTuplesInserted; ++i) {
      tuples.putLong(0, i);
      tuples.putString(1, "row" + i);
    }
    final TupleSource source = new TupleSource(tuples);

    /*** TEST PHASE 1: Insert all the tuples. ***/
    /* Create the shuffle producer. */
    final ExchangePairID shuffleId = ExchangePairID.newID();

    final GenericShuffleProducer scatter =
        new GenericShuffleProducer(source, shuffleId, new int[] { workerIDs[0], workerIDs[1] },
            new RoundRobinPartitionFunction(workerIDs.length));

    /* ... and the corresponding shuffle consumer. */
    final GenericShuffleConsumer gather = new GenericShuffleConsumer(schema, shuffleId, new int[] { MASTER_ID });

    final RelationKey tuplesRRKey = RelationKey.of("test", "test", "tuples_rr");

    /* Create the Insert operator */
    final DbInsert insert = new DbInsert(gather, tuplesRRKey, true);

    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    for (final int i : workerIDs) {
      workerPlans.put(i, new SingleQueryPlanWithArgs(new RootOperator[] { insert }));
    }
    SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(scatter);

    server.submitQueryPlan("", "", "", serverPlan, workerPlans).sync();

    /*** TEST PHASE 2: Count them up, make sure the answer agrees. ***/
    /* Create the worker plan: DbQueryScan with count, then send it to master. */
    Schema countResultSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("localCount"));
    final DbQueryScan scanCount =
        new DbQueryScan("SELECT COUNT(*) FROM " + tuplesRRKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
            countResultSchema);

    final ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer send = new CollectProducer(scanCount, collectId, 0);
    workerPlans.clear();
    for (final int i : workerIDs) {
      workerPlans.put(i, new SingleQueryPlanWithArgs(new RootOperator[] { send }));
    }
    /* Create the Server plan: CollectConsumer and Sum. */
    final CollectConsumer receive = new CollectConsumer(countResultSchema, collectId, workerIDs);
    Aggregate sumCount =
        new Aggregate(receive, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_COUNT });

    serverPlan = new SingleQueryPlanWithArgs(new DataOutput(sumCount, DatasetFormat.TSV));

    /* Actually dispatch the worker plans. */
    /* Start the query and collect the results. */
    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);

    TupleBatchBuffer resultTBB =
        TestUtils.parseTSV(((MasterQueryPartition) qf.getQuery()).getResultStream(), ((MasterQueryPartition) qf
            .getQuery()).getResultSchema());
    TupleBatch result = resultTBB.popAny();

    /* Sanity-check the results, sum them, then confirm. */
    assertEquals(workerIDs.length, result.getLong(0, 0));

    LOGGER.debug("numTuplesInsert=" + numTuplesInserted + ", sum=" + result.getObject(0, 0));
    assertEquals(numTuplesInserted, result.getLong(1, 0));

  }
}
