package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

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
  public void testTwoQueriesFirstFails() throws Exception {
    /* One query which will fail. */
    QueryPlan q1 = TestUtils.failOnMaster();

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
}
