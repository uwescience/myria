package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants.PROFILING_MODE;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.parallel.Sequence;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;

public class ServerTest extends SystemTestBase {
  /**
   * Test that profiling mode fails, because we use SQLite.
   */
  @Test(expected = DbException.class)
  public void testProfilingWithSQLite() throws Exception {
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan plan = new SubQuery(serverPlan, new HashMap<Integer, SubQueryPlan>());
    QueryEncoding query = new QueryEncoding();
    query.rawQuery = "testDatalog";
    query.logicalRa = "testRa";
    query.profilingMode = PROFILING_MODE.QUERY;
    try {
      server.getQueryManager().submitQuery(query, plan);
    } catch (DbException e) {
      assertTrue(e.getMessage().contains("Profiling mode is only supported when using Postgres as the storage system."));
      throw e;
    }
  }

  /**
   * Test that profiling mode fails when we use multiple queries.
   */
  @Test(expected = DbException.class)
  public void testProfilingWithMultipleQueries() throws Exception {
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    QueryPlan frag = new SubQuery(serverPlan, new HashMap<Integer, SubQueryPlan>());
    QueryPlan plan = new Sequence(ImmutableList.of(frag, frag));
    QueryEncoding query = new QueryEncoding();
    query.rawQuery = "testDatalog";
    query.logicalRa = "testRa";
    query.profilingMode = PROFILING_MODE.QUERY;

    try {
      server.getQueryManager().submitQuery(query, plan);
    } catch (DbException e) {
      assertTrue(e.getMessage().contains("Profiling mode is not supported for plans"));
      throw e;
    }
  }
}
