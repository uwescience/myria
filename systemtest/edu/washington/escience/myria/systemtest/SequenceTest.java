package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.failures.InitFailureInjector;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.parallel.Sequence;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.ErrorUtils;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.TestUtils;

public class SequenceTest extends SystemTestBase {

  @Test
  public void testSequence() throws Exception {
    final int numVals = TupleBatch.BATCH_SIZE + 2;
    TupleSource source = new TupleSource(TestUtils.range(numVals).getAll());
    Schema testSchema = source.getSchema();
    RelationKey storage = RelationKey.of("test", "testi", "step1");
    PartitionFunction pf = new SingleFieldHashPartitionFunction(workerIDs.length, 0);

    QueryPlan first = TestUtils.insertRelation(source, storage, pf, workerIDs);

    /* Second task: count the number of tuples. */
    DbQueryScan scan = new DbQueryScan(storage, testSchema);
    Aggregate localCount =
        new Aggregate(scan, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));
    ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer coll =
        new CollectProducer(localCount, collectId, MyriaConstants.MASTER_ID);

    final CollectConsumer cons =
        new CollectConsumer(Schema.ofFields(Type.LONG_TYPE, "_COUNT0_"), collectId, workerIDs);
    Aggregate masterSum =
        new Aggregate(cons, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, masterSum);
    SinkRoot root = new SinkRoot(queueStore);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(coll));
    }
    QueryPlan second = new SubQuery(new SubQueryPlan(root), workerPlans);

    /* Combine first and second into two queries, one after the other. */
    QueryPlan all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query and compute its ID. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.rawQuery = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.getQueryManager().submitQuery(encoding, all);
    long queryId = qf.getQueryId();
    /* Wait for the query to finish, succeed, and check the result. */
    qf.get();
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
    assertEquals(numVals, server.getDatasetStatus(storage).getNumTuples());

    List<TupleBatch> tbs = Lists.newLinkedList(receivedTupleBatches);
    assertEquals(1, tbs.size());
    TupleBatch tb = tbs.get(0);
    assertEquals(numVals, tb.getLong(0, 0));
  }

  @Test
  public void testNestedSequenceBug() throws Exception {
    final int numVals = TupleBatch.BATCH_SIZE + 2;
    TupleSource source = new TupleSource(TestUtils.range(numVals).getAll());
    Schema testSchema = source.getSchema();
    RelationKey storage = RelationKey.of("test", "testNestedSequenceBug", "data");

    ExchangePairID shuffleId = ExchangePairID.newID();
    final GenericShuffleProducer sp =
        new GenericShuffleProducer(
            source,
            shuffleId,
            workerIDs,
            new SingleFieldHashPartitionFunction(workerIDs.length, 0));

    final GenericShuffleConsumer cc = new GenericShuffleConsumer(testSchema, shuffleId, workerIDs);
    final DbInsert insert = new DbInsert(cc, storage, true);

    /* First task: create, shuffle, insert the tuples. */
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(new RootOperator[] {insert, sp}));
    }
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan first =
        new Sequence(ImmutableList.<QueryPlan>of(new SubQuery(serverPlan, workerPlans)));

    /* Second task: count the number of tuples. */
    DbQueryScan scan = new DbQueryScan(storage, testSchema);
    Aggregate localCount =
        new Aggregate(scan, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));
    ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer coll =
        new CollectProducer(localCount, collectId, MyriaConstants.MASTER_ID);

    final CollectConsumer cons =
        new CollectConsumer(Schema.ofFields(Type.LONG_TYPE, "_COUNT0_"), collectId, workerIDs);
    Aggregate masterSum =
        new Aggregate(cons, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
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
    encoding.rawQuery = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.getQueryManager().submitQuery(encoding, all);
    long queryId = qf.getQueryId();
    /* Wait for the query to finish, succeed, and check the result. */
    qf.get();
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);
    long expectedTuples = numVals * workerIDs.length;
    assertEquals(expectedTuples, server.getDatasetStatus(storage).getNumTuples());

    List<TupleBatch> tbs = Lists.newLinkedList(receivedTupleBatches);
    assertEquals(1, tbs.size());
    TupleBatch tb = tbs.get(0);
    assertEquals(expectedTuples, tb.getLong(0, 0));
  }

  @Test(expected = ExecutionException.class)
  public void testSequenceFailSubQuery2() throws Exception {
    /* First task: do nothing. */
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(new RootOperator[] {new SinkRoot(new EOSSource())}));
    }
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan first = new SubQuery(serverPlan, workerPlans);

    /* Second task: crash just the first worker. */
    workerPlans = new HashMap<>();
    workerPlans.put(
        workerIDs[0],
        new SubQueryPlan(
            new RootOperator[] {new SinkRoot(new InitFailureInjector(new EOSSource()))}));
    QueryPlan second = new SubQuery(serverPlan, workerPlans);

    /* Combine first and second into two queries, one after the other. */
    QueryPlan all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.rawQuery = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.getQueryManager().submitQuery(encoding, all);

    /* Wait for query to finish, expecting an exception. */
    try {
      qf.get();
      fail();
    } catch (Exception e) {
      /*
       * Assert both 1) that the cause of the exception is in the stack trace and 2) that this message is visible via
       * the API. See https://github.com/uwescience/myria/issues/542
       */
      assertTrue(ErrorUtils.getStackTrace(e).contains("Failure in init"));
      String message = server.getQueryManager().getQueryStatus(qf.getQueryId()).message;
      assertNotNull(message);
      assertTrue(message.contains("Failure in init"));
      throw e;
    }
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
    assertEquals(3, server.getDatasetStatus(origKey).getNumTuples());

    /* First job: select just the value column and insert it to a new intermediate relation. */
    TableScanEncoding scan = new TableScanEncoding();

    int SCAN = 0, APPLY = 1, INSERT = 2, PROD = 3, CONS = 4, AGG = 5;

    scan.opId = SCAN;
    scan.relationKey = origKey;
    ApplyEncoding apply = new ApplyEncoding();
    apply.opId = APPLY;
    apply.argChild = scan.opId;
    apply.emitExpressions = ImmutableList.of(new Expression("value", new VariableExpression(0)));
    DbInsertEncoding insert = new DbInsertEncoding();
    insert.opId = INSERT;
    insert.argChild = apply.opId;
    insert.relationKey = interKey;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding fragment = new PlanFragmentEncoding();
    fragment.operators = ImmutableList.of(scan, apply, insert);
    SubQueryEncoding firstJson = new SubQueryEncoding(ImmutableList.of(fragment));

    /* Second job: Sum the values in that column. */
    // Fragment 1: scan and produce
    scan = new TableScanEncoding();
    scan.opId = SCAN;
    scan.relationKey = interKey;
    CollectProducerEncoding prod = new CollectProducerEncoding();
    prod.opId = PROD;
    prod.argChild = scan.opId;
    fragment = new PlanFragmentEncoding();
    fragment.operators = ImmutableList.of(scan, prod);
    // Fragment 2: consume, agg, insert
    CollectConsumerEncoding cons = new CollectConsumerEncoding();
    cons.opId = CONS;
    cons.argOperatorId = prod.opId;
    AggregateEncoding agg = new AggregateEncoding();
    agg.argChild = cons.opId;
    agg.opId = AGG;
    agg.aggregators =
        new AggregatorFactory[] {new SingleColumnAggregatorFactory(0, AggregationOp.SUM)};
    insert = new DbInsertEncoding();
    insert.opId = INSERT;
    insert.argChild = agg.opId;
    insert.relationKey = resultKey;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding fragment2 = new PlanFragmentEncoding();
    fragment2.operators = ImmutableList.of(cons, agg, insert);
    SubQueryEncoding secondJson = new SubQueryEncoding(ImmutableList.of(fragment, fragment2));

    // Sequence of firstJson, secondJson
    SequenceEncoding seq = new SequenceEncoding();
    seq.plans = ImmutableList.<SubPlanEncoding>of(firstJson, secondJson);

    QueryEncoding query = new QueryEncoding();
    query.plan = seq;
    query.logicalRa = "sequence json test";
    query.rawQuery = query.logicalRa;

    String queryString = writer.writeValueAsString(query);

    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryString);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
    assertEquals(1, server.getDatasetStatus(resultKey).getNumTuples());

    String ret =
        JsonAPIUtils.download(
            "localhost",
            masterDaemonPort,
            resultKey.getUserName(),
            resultKey.getProgramName(),
            resultKey.getRelationName(),
            "csv");
    assertEquals("sum_value\r\n9\r\n", ret);
  }

  @Test
  public void testNestedSequence() throws Exception {
    final int numVals = TupleBatch.BATCH_SIZE + 2;
    final int numCopies = 3;
    TupleBatchBuffer data = TestUtils.range(numVals);
    Schema schema = data.getSchema();
    RelationKey dataKey = RelationKey.of("test", "testi", "step");
    List<RootOperator[]> seqs = Lists.newLinkedList();

    for (int i = 0; i < numCopies; ++i) {
      TupleSource source = new TupleSource(data.getAll());
      ExchangePairID shuffleId = ExchangePairID.newID();
      final GenericShuffleProducer sp =
          new GenericShuffleProducer(
              source,
              shuffleId,
              workerIDs,
              new SingleFieldHashPartitionFunction(workerIDs.length, 0));

      final GenericShuffleConsumer cc = new GenericShuffleConsumer(schema, shuffleId, workerIDs);

      /* Only overwrite for the first seq when i == 0. After, append. */
      final DbInsert insert = new DbInsert(cc, dataKey, i == 0);
      seqs.add(new RootOperator[] {insert, sp});
    }

    /* First Sequence: do the numCopies inserts, one after the other. */
    List<QueryPlan> plans = Lists.newLinkedList();
    for (RootOperator[] op : seqs) {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (int i : workerIDs) {
        workerPlans.put(i, new SubQueryPlan(op));
      }
      SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
      plans.add(new SubQuery(serverPlan, workerPlans));
    }
    QueryPlan first = new Sequence(plans);

    /* Task to run after the first Sequence: Count the number of tuples. */
    DbQueryScan scan = new DbQueryScan(dataKey, schema);
    Aggregate localCount =
        new Aggregate(scan, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));
    ExchangePairID collectId = ExchangePairID.newID();
    final CollectProducer coll =
        new CollectProducer(localCount, collectId, MyriaConstants.MASTER_ID);

    final CollectConsumer cons =
        new CollectConsumer(Schema.ofFields(Type.LONG_TYPE, "_COUNT0_"), collectId, workerIDs);
    Aggregate masterSum =
        new Aggregate(cons, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, masterSum);
    SinkRoot root = new SinkRoot(queueStore);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int i : workerIDs) {
      workerPlans.put(i, new SubQueryPlan(coll));
    }
    // Stick in a trivial Sequence of one subquery for good measure
    QueryPlan second =
        new Sequence(
            ImmutableList.<QueryPlan>of(new SubQuery(new SubQueryPlan(root), workerPlans)));

    /* Combine first and second into two queries, one after the other. */
    QueryPlan all = new Sequence(ImmutableList.of(first, second));

    /* Submit the query and compute its ID. */
    QueryEncoding encoding = new QueryEncoding();
    encoding.rawQuery = "test";
    encoding.logicalRa = "test";
    QueryFuture qf = server.getQueryManager().submitQuery(encoding, all);
    long queryId = qf.getQueryId();
    /* Wait for the query to finish, succeed, and check the result. */
    Query query = qf.get();
    assertEquals(Status.SUCCESS, query.getStatus());
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);

    long expectedNumTuples = numVals * numCopies * workerIDs.length;

    List<TupleBatch> tbs = Lists.newLinkedList(receivedTupleBatches);
    assertEquals(1, tbs.size());
    TupleBatch tb = tbs.get(0);
    assertEquals(expectedNumTuples, tb.getLong(0, 0));
    /* Ensure that the number of tuples matches our expectation. */
    assertEquals(expectedNumTuples, server.getDatasetStatus(dataKey).getNumTuples());
  }
}
