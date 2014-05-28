package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.AggregateEncoding;
import edu.washington.escience.myria.api.encoding.ApplyEncoding;
import edu.washington.escience.myria.api.encoding.CollectConsumerEncoding;
import edu.washington.escience.myria.api.encoding.CollectProducerEncoding;
import edu.washington.escience.myria.api.encoding.DatasetEncoding;
import edu.washington.escience.myria.api.encoding.DbInsertEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.api.encoding.TableScanEncoding;
import edu.washington.escience.myria.api.encoding.plan.SequenceEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubPlanEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.io.ByteArraySource;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.operator.failures.InitFailureInjector;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.Sequence;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.JsonAPIUtils;

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
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(new RootOperator[] { insert, sp }));
    }
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan first = new SubQuery(serverPlan, workerPlans);

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
      workerPlans.put(i, new SubQueryPlan(coll));
    }
    QueryPlan second = new SubQuery(new SubQueryPlan(root), workerPlans);

    /* Combine first and second into two queries, one after the other. */
    QueryPlan all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query and compute its ID. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.profilingMode = false;
    encoding.rawDatalog = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.submitQuery(encoding, all);
    long queryId = qf.getQueryId();
    /* Wait for the query to finish, succeed, and check the result. */
    qf.get();
    QueryStatusEncoding status = server.getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);

    List<TupleBatch> tbs = Lists.newLinkedList(receivedTupleBatches);
    assertEquals(1, tbs.size());
    TupleBatch tb = tbs.get(0);
    assertEquals(numVals * workerIDs.length, tb.getLong(0, 0));
  }

  @Test(expected = ExecutionException.class)
  public void testSequenceFailSubQuery2() throws Exception {
    /* First task: do nothing. */
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(new RootOperator[] { new SinkRoot(new EOSSource()) }));
    }
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan first = new SubQuery(serverPlan, workerPlans);

    /* Second task: crash just the first worker. */
    workerPlans = new HashMap<>();
    workerPlans.put(workerIDs[0], new SubQueryPlan(new RootOperator[] { new SinkRoot(new InitFailureInjector(
        new EOSSource())) }));
    QueryPlan second = new SubQuery(serverPlan, workerPlans);

    /* Combine first and second into two queries, one after the other. */
    QueryPlan all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.profilingMode = false;
    encoding.rawDatalog = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.submitQuery(encoding, all);

    /* Wait for query to finish, expecting an exception. */
    qf.get();
  }

  @Test
  public void testJsonSequence() throws Exception {
    ObjectWriter writer = MyriaJsonMapperProvider.getWriter();

    Schema schema = Schema.ofFields("value", Type.INT_TYPE, "label", Type.STRING_TYPE);
    RelationKey origKey = RelationKey.of("test", "sequence", "json");
    RelationKey interKey = RelationKey.of("test", "sequence", "jsonValue");
    RelationKey resultKey = RelationKey.of("test", "sequence", "jsonSumValues");

    /* Begin with ingest */
    byte[] data = Joiner.on('\n').join("1,a", "3,b", "5,a").getBytes();
    DataSource source = new ByteArraySource(data);
    DatasetEncoding dataset = new DatasetEncoding();
    dataset.source = source;
    dataset.schema = schema;
    dataset.relationKey = origKey;
    String ingestString = writer.writeValueAsString(dataset);
    JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestString);

    /* First job: select just the value column and insert it to a new intermediate relation. */
    TableScanEncoding scan = new TableScanEncoding();
    scan.opId = "scan";
    scan.relationKey = origKey;
    ApplyEncoding apply = new ApplyEncoding();
    apply.opId = "apply";
    apply.argChild = scan.opId;
    apply.emitExpressions = ImmutableList.of(new Expression("value", new VariableExpression(0)));
    DbInsertEncoding insert = new DbInsertEncoding();
    insert.opId = "insert";
    insert.argChild = apply.opId;
    insert.relationKey = interKey;
    PlanFragmentEncoding fragment = new PlanFragmentEncoding();
    fragment.operators = ImmutableList.of(scan, apply, insert);
    SubQueryEncoding firstJson = new SubQueryEncoding(ImmutableList.of(fragment));

    /* Second job: Sum the values in that column. */
    // Fragment 1: scan and produce
    scan = new TableScanEncoding();
    scan.opId = "scan";
    scan.relationKey = interKey;
    CollectProducerEncoding prod = new CollectProducerEncoding();
    prod.opId = "prod";
    prod.argChild = scan.opId;
    fragment = new PlanFragmentEncoding();
    fragment.operators = ImmutableList.of(scan, prod);
    // Fragment 2: consume, agg, insert
    CollectConsumerEncoding cons = new CollectConsumerEncoding();
    cons.opId = "cons";
    cons.argOperatorId = prod.opId;
    AggregateEncoding agg = new AggregateEncoding();
    agg.argChild = cons.opId;
    agg.opId = "agg";
    agg.argAggFields = new int[] { 0 };
    agg.argAggOperators = ImmutableList.of((List<String>) ImmutableList.of("AGG_OP_SUM"));
    insert = new DbInsertEncoding();
    insert.opId = "insert";
    insert.argChild = agg.opId;
    insert.relationKey = resultKey;
    PlanFragmentEncoding fragment2 = new PlanFragmentEncoding();
    fragment2.operators = ImmutableList.of(cons, agg, insert);
    SubQueryEncoding secondJson = new SubQueryEncoding(ImmutableList.of(fragment, fragment2));

    // Sequence of firstJson, secondJson
    SequenceEncoding seq = new SequenceEncoding();
    seq.plans = ImmutableList.<SubPlanEncoding> of(firstJson, secondJson);

    QueryEncoding query = new QueryEncoding();
    query.plan = seq;
    query.logicalRa = "sequence json test";
    query.rawDatalog = query.logicalRa;

    String queryString = writer.writeValueAsString(query);

    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryString);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);

    String ret =
        JsonAPIUtils.download("localhost", masterDaemonPort, resultKey.getUserName(), resultKey.getProgramName(),
            resultKey.getRelationName(), "csv");
    assertEquals("sum_value\r\n9\r\n", ret);
  }
}
