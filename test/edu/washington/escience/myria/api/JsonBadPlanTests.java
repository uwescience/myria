package edu.washington.escience.myria.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.AbstractConsumerEncoding;
import edu.washington.escience.myria.api.encoding.AbstractProducerEncoding;
import edu.washington.escience.myria.api.encoding.CollectConsumerEncoding;
import edu.washington.escience.myria.api.encoding.CollectProducerEncoding;
import edu.washington.escience.myria.api.encoding.EmptyRelationEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.SinkRootEncoding;
import edu.washington.escience.myria.api.encoding.UnionAllEncoding;

/**
 * Test of illegal plans submitted via the API.
 */
public class JsonBadPlanTests {

  static Logger LOGGER = LoggerFactory.getLogger(JsonBadPlanTests.class);

  @Test
  public void fragmentMissingConsumerTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    AbstractProducerEncoding<?> prod = new CollectProducerEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    prod.opId = 1;
    prod.argChild = empty.opId;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty, prod);

    try {
      QueryConstruct.sanityCheckEdges(ImmutableList.of(frag));
      fail();
    } catch (IllegalArgumentException e) {
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

    try {
      QueryConstruct.sanityCheckEdges(ImmutableList.of(frag));
      fail();
    } catch (IllegalArgumentException e) {
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
    ua.argChildren = new Integer[] {cons1.opId, cons2.opId};
    ua.opId = 4;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.opId = 5;
    sink.argChild = ua.opId;
    PlanFragmentEncoding consFrag = PlanFragmentEncoding.of(cons1, cons2, ua, sink);

    try {
      QueryConstruct.sanityCheckEdges(ImmutableList.of(prodFrag, consFrag));
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("Producer 1 only supports a single consumer, not 2");
    }
  }

  @Test
  public void fragmentDuplicateProducerTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    AbstractProducerEncoding<?> prod = new CollectProducerEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    prod.opId = 1;
    prod.argChild = empty.opId;
    PlanFragmentEncoding prodFrag = PlanFragmentEncoding.of(empty, prod);

    EmptyRelationEncoding empty2 = new EmptyRelationEncoding();
    AbstractProducerEncoding<?> prod2 = new CollectProducerEncoding();
    empty2.opId = 2;
    empty2.schema = Schema.ofFields("x", Type.LONG_TYPE);
    prod2.opId = 1;
    prod2.argChild = empty2.opId;
    PlanFragmentEncoding prodFrag2 = PlanFragmentEncoding.of(empty2, prod2);

    AbstractConsumerEncoding<?> cons1 = new CollectConsumerEncoding();
    cons1.opId = 4;
    cons1.argOperatorId = prod.opId;
    AbstractConsumerEncoding<?> cons2 = new CollectConsumerEncoding();
    cons2.opId = 5;
    cons2.argOperatorId = 3;
    UnionAllEncoding ua = new UnionAllEncoding();
    ua.argChildren = new Integer[] {cons1.opId, cons2.opId};
    ua.opId = 6;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.opId = 7;
    sink.argChild = ua.opId;
    PlanFragmentEncoding consFrag = PlanFragmentEncoding.of(cons1, cons2, ua, sink);

    try {
      QueryConstruct.sanityCheckEdges(ImmutableList.of(prodFrag, prodFrag2, consFrag));
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage())
          .contains("Two different operators cannot produce the same opId 1.");
    }
  }
}
