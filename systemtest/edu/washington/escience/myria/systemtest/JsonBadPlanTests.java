package edu.washington.escience.myria.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.AbstractConsumerEncoding;
import edu.washington.escience.myria.api.encoding.AbstractProducerEncoding;
import edu.washington.escience.myria.api.encoding.CollectConsumerEncoding;
import edu.washington.escience.myria.api.encoding.CollectProducerEncoding;
import edu.washington.escience.myria.api.encoding.EmptyRelationEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.SinkRootEncoding;
import edu.washington.escience.myria.api.encoding.UnionAllEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * Test of illegal plans submitted via the API.
 */
public class JsonBadPlanTests extends SystemTestBase {

  static Logger LOGGER = LoggerFactory.getLogger(JsonBadPlanTests.class);

  private HttpURLConnection submitQuery(final QueryEncoding query) throws IOException {
    ObjectWriter writer = MyriaJsonMapperProvider.getWriter();
    String queryString = writer.writeValueAsString(query);
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryString);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    return conn;
  }

  @Test
  public void fragmentNoRootTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment no root test";
    query.rawQuery = query.logicalRa;

    try {
      submitQuery(query);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("No RootOperator detected");
    }
  }

  @Test
  public void fragmentTwoRootsTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    SinkRootEncoding sink1 = new SinkRootEncoding();
    SinkRootEncoding sink2 = new SinkRootEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    sink1.opId = 1;
    sink1.argChild = empty.opId;
    sink2.opId = 2;
    sink2.argChild = empty.opId;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty, sink1, sink2);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment two roots test";
    query.rawQuery = query.logicalRa;

    try {
      submitQuery(query);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("Multiple RootOperator detected");
    }
  }

  @Test
  public void fragmentMissingConsumerTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    AbstractProducerEncoding<?> prod = new CollectProducerEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    prod.opId = 1;
    prod.argChild = empty.opId;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty, prod);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment missing consumer test";
    query.rawQuery = query.logicalRa;

    try {
      submitQuery(query);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("Missing consumer(s) for producer(s): [1]");
    }
  }

  @Test
  public void fragmentMissingProducerTest() throws Exception {
    AbstractConsumerEncoding<?> cons = new CollectConsumerEncoding();
    cons.opId = 0;
    cons.argOperatorId = 2;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.opId = 1;
    sink.argChild = cons.opId;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(cons, sink);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment missing producer test";
    query.rawQuery = query.logicalRa;

    try {
      submitQuery(query);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("Missing producer(s) for consumer(s): [2]");
    }
  }

  @Test
  public void fragmentExtraConsumerTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    AbstractProducerEncoding<?> prod = new CollectProducerEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    prod.opId = 1;
    prod.argChild = empty.opId;
    PlanFragmentEncoding prodFrag = PlanFragmentEncoding.of(empty, prod);

    AbstractConsumerEncoding<?> cons1 = new CollectConsumerEncoding();
    cons1.opId = 2;
    cons1.argOperatorId = prod.opId;
    AbstractConsumerEncoding<?> cons2 = new CollectConsumerEncoding();
    cons2.opId = 3;
    cons2.argOperatorId = prod.opId;
    UnionAllEncoding ua = new UnionAllEncoding();
    ua.argChildren = new Integer[] { cons1.opId, cons2.opId };
    ua.opId = 4;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.opId = 5;
    sink.argChild = ua.opId;
    PlanFragmentEncoding consFrag = PlanFragmentEncoding.of(cons1, cons2, ua, sink);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(prodFrag, consFrag));
    query.logicalRa = "Fragment extra consumer test";
    query.rawQuery = query.logicalRa;

    try {
      submitQuery(query);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("Producer 1 only supports a single consumer, not 2");
    }
  }
}
