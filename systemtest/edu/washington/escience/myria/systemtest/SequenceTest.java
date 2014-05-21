package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.parallel.meta.Fragment;
import edu.washington.escience.myria.parallel.meta.MetaTask;
import edu.washington.escience.myria.parallel.meta.Sequence;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class SequenceTest extends SystemTestBase {

  @Test
  public void testSequence() throws Exception {
    Schema testSchema = Schema.ofFields(Type.INT_TYPE, "val");
    TupleBatchBuffer sourceBuffer = new TupleBatchBuffer(testSchema);
    final int numVals = TupleBatch.BATCH_SIZE + 2;
    for (int i = 0; i < numVals; ++i) {
      sourceBuffer.putInt(0, i);
    }
    final TupleSource source = new TupleSource(sourceBuffer.getAll());

    ExchangePairID shuffleId = ExchangePairID.newID();
    final GenericShuffleProducer sp =
        new GenericShuffleProducer(source, shuffleId, workerIDs, new SingleFieldHashPartitionFunction(workerIDs.length,
            0));

    final GenericShuffleConsumer cc = new GenericShuffleConsumer(testSchema, shuffleId, workerIDs);
    RelationKey storage = RelationKey.of("test", "testi", "step1");
    final DbInsert insert = new DbInsert(cc, storage, true);

    /* First task: create, shuffle, insert the tuples. */
    Map<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SingleQueryPlanWithArgs(new RootOperator[] { insert, sp }));
    }
    SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(new EOSSource()));
    MetaTask first = new Fragment(serverPlan, workerPlans);

    /* Second task: count the number of tuples. */
    DbQueryScan scan = new DbQueryScan(storage, testSchema);
    Aggregate localCount = new Aggregate(scan, new int[] { 0 }, new int[] { Aggregator.AGG_OP_COUNT });
    ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer coll = new CollectProducer(localCount, collectId, MyriaConstants.MASTER_ID);

    final CollectConsumer cons = new CollectConsumer(Schema.ofFields(Type.LONG_TYPE, "_COUNT0_"), collectId, workerIDs);
    Aggregate masterSum = new Aggregate(cons, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, masterSum);
    SinkRoot root = new SinkRoot(queueStore);
    workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SingleQueryPlanWithArgs(coll));
    }
    MetaTask second = new Fragment(new SingleQueryPlanWithArgs(root), workerPlans);

    /* Combine first and second into two queries, one after the other. */
    MetaTask all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query and compute its ID. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.profilingMode = false;
    encoding.rawDatalog = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.submitQuery(encoding, all);
    long queryId = qf.getQuery().getTaskId().getQueryId();

    /* Wait for the query to finish, succeed, and check the result. */
    while (!server.queryCompleted(queryId)) {
      Thread.sleep(100);
    }
    QueryStatusEncoding status = server.getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);

    List<TupleBatch> tbs = Lists.newLinkedList(receivedTupleBatches);
    assertEquals(1, tbs.size());
    TupleBatch tb = tbs.get(0);
    assertEquals(numVals * workerIDs.length, tb.getLong(0, 0));
  }
}
