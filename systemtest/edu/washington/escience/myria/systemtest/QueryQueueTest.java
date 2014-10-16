package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestUtils;

public class QueryQueueTest extends SystemTestBase {

  @Test
  public void testTwoQueries() throws Exception {
    TupleSource source1 = new TupleSource(TestUtils.range(TupleBatch.BATCH_SIZE * 250));
    TupleSource source2 = new TupleSource(TestUtils.range(10));
    final RelationKey table1Key = RelationKey.of("test", "test", "bigtable");
    final RelationKey table2Key = RelationKey.of("test", "test", "tinytable");
    PartitionFunction pf = new SingleFieldHashPartitionFunction(workerIDs.length, 0);
    /* One long query. */
    QueryPlan q1 = TestUtils.insertRelation(source1, table1Key, pf, workerIDs);
    /* One very short query. */
    QueryPlan q2 = TestUtils.insertRelation(source2, table2Key, pf, workerIDs);

    QueryFuture qf1 = server.getQueryManager().submitQuery("long query 1", "long query 1", "long query 1", q1, null);
    QueryFuture qf2 = server.getQueryManager().submitQuery("short query 2", "short query 2", "short query 1", q2, null);
    Query query1 = qf1.get();
    Query query2 = qf2.get();
    assertEquals(query1.getMessage(), query1.getStatus(), Status.SUCCESS);
    assertEquals(query2.getMessage(), query2.getStatus(), Status.SUCCESS);
    /* The goal: query 2 should have been started after query 1 finished. */
    assertTrue(query1.getEndTime().compareTo(query2.getStartTime()) < 0);
  }

  @Test
  public void testTwoQueriesFirstFailsOnMaster() throws Exception {
    /* One query which will fail. */
    QueryPlan q1 = TestUtils.failOnMasterInit();

    /* A short insert query which will succeed. */
    TupleSource source2 = new TupleSource(TestUtils.range(10));
    final RelationKey table2Key = RelationKey.of("test", "test", "tinytable");
    PartitionFunction pf = new SingleFieldHashPartitionFunction(workerIDs.length, 0);
    QueryPlan q2 = TestUtils.insertRelation(source2, table2Key, pf, workerIDs);

    QueryFuture qf1 = server.getQueryManager().submitQuery("fail query 1", "fail query 1", "fail query 1", q1, null);
    QueryFuture qf2 = server.getQueryManager().submitQuery("short query 2", "short query 2", "short query 1", q2, null);
    QueryStatusEncoding query1 = null;
    try {
      qf1.get();
      fail("Should not reach here!");
    } catch (ExecutionException e) {
      /* This is expected. */
      query1 = server.getQueryManager().getQueryStatus(qf1.getQueryId());
    }
    assertNotNull(query1);
    Query query2 = qf2.get();
    assertEquals(query1.status, Status.ERROR);
    assertEquals(query2.getMessage(), query2.getStatus(), Status.SUCCESS);
    /* The goal: query 2 should have been started after query 1 finished. */
    assertTrue(query1.finishTime.compareTo(query2.getStartTime()) < 0);
  }

  @Test
  public void testTwoQueriesFirstFailsOnWorker() throws Exception {
    /* One query which will fail. */
    QueryPlan q1 = TestUtils.failOnFirstWorkerInit(workerIDs);

    /* A short insert query which will succeed. */
    TupleSource source2 = new TupleSource(TestUtils.range(10));
    final RelationKey table2Key = RelationKey.of("test", "test", "tinytable");
    PartitionFunction pf = new SingleFieldHashPartitionFunction(workerIDs.length, 0);
    QueryPlan q2 = TestUtils.insertRelation(source2, table2Key, pf, workerIDs);

    QueryFuture qf1 = server.getQueryManager().submitQuery("fail query 1", "fail query 1", "fail query 1", q1, null);
    QueryFuture qf2 = server.getQueryManager().submitQuery("short query 2", "short query 2", "short query 1", q2, null);
    QueryStatusEncoding query1 = null;
    try {
      qf1.get();
      fail("Should not reach here!");
    } catch (ExecutionException e) {
      /* This is expected. */
      query1 = server.getQueryManager().getQueryStatus(qf1.getQueryId());
    }
    assertNotNull(query1);
    Query query2 = qf2.get();
    assertEquals(query1.status, Status.ERROR);
    assertEquals(query2.getMessage(), query2.getStatus(), Status.SUCCESS);
    /* The goal: query 2 should have been started after query 1 finished. */
    assertTrue(query1.finishTime.compareTo(query2.getStartTime()) < 0);
  }

  @Test
  public void testTooManyQueries() throws Exception {
    final int numQueries = MyriaConstants.MAX_ACTIVE_QUERIES * 3;
    QueryFuture[] qfs = new QueryFuture[numQueries];
    PartitionFunction pf = new SingleFieldHashPartitionFunction(workerIDs.length, 0);
    boolean broken = false;
    int numSubmitted = 0;
    for (int i = 0; i < numQueries; ++i) {
      TupleSource source = new TupleSource(TestUtils.range(10));
      RelationKey tableKey = RelationKey.of("test", "test", "tinytable" + i);
      QueryPlan q = TestUtils.insertRelation(source, tableKey, pf, workerIDs);
      try {
        qfs[i] = server.getQueryManager().submitQuery("fail query 1", "fail query 1", "fail query 1", q, null);
        numSubmitted = i;
      } catch (DbException e) {
        /* Expect a "too many active queries" message. */
        assertTrue(e.getMessage().contains("Cannot submit query"));
        broken = true;
        break;
      }
    }

    for (int i = 0; i < numSubmitted; ++i) {
      qfs[i].get();
    }
    assertTrue("expected that we broke out of the loop by submitting too many queries, but we did not!", broken);
    assertTrue("expect number of successful query submissions " + numSubmitted + " to be less than " + numQueries,
        numSubmitted < numQueries);
    assertNull(qfs[numSubmitted + 1]);
  }
}
