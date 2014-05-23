package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.MetaTask;
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
    MetaTask plan = new SubQuery(serverPlan, new HashMap<Integer, SubQueryPlan>());
    QueryEncoding query = new QueryEncoding();
    query.rawDatalog = "testDatalog";
    query.logicalRa = "testRa";
    query.profilingMode = true;
    try {
      server.submitQuery(query, plan);
    } catch (DbException e) {
      assertTrue(e.getMessage().contains("Profiling mode is not supported"));
      throw e;
    }
  }

  /**
   * Test that profiling mode fails when we use multiple queries.
   */
  @Test(expected = DbException.class)
  public void testProfilingWithMultipleQueries() throws Exception {
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    MetaTask frag = new SubQuery(serverPlan, new HashMap<Integer, SubQueryPlan>());
    MetaTask plan = new Sequence(ImmutableList.of(frag, frag));
    QueryEncoding query = new QueryEncoding();
    query.rawDatalog = "testDatalog";
    query.logicalRa = "testRa";
    query.profilingMode = true;

    try {
      server.submitQuery(query, plan);
    } catch (DbException e) {
      assertTrue(e.getMessage().contains("Profiling mode is not supported for plans"));
      throw e;
    }
  }
}
