package edu.washington.escience.myria.parallel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.SinkRoot;

/**
 * A {@link QueryPlan} that runs a single subquery. Note that a {@link JsonSubQuery} cannot have a {@link QueryPlan} as
 * a child, but rather can only accept a physical JSON subquery as a set of fragments.
 */
public final class JsonSubQuery extends QueryPlan {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonSubQuery.class);
  /** The json query to be executed. */
  private final List<PlanFragmentEncoding> fragments;

  /**
   * @return the fragments of the query.
   */
  protected List<PlanFragmentEncoding> getFragments() {
    return ImmutableList.copyOf(fragments);
  }

  /**
   * Construct a {@link QueryPlan} that runs the given subquery. The subquery will be instantiated using
   * {@link QueryConstruct#instantiate(List, edu.washington.escience.myria.parallel.Server)}.
   *
   * @param fragments the JSON query to be executed, broken into fragments
   * @see QueryConstruct#instantiate(List, edu.washington.escience.myria.parallel.Server)
   */
  public JsonSubQuery(final List<PlanFragmentEncoding> fragments) {
    this.fragments = Objects.requireNonNull(fragments, "fragments");
  }

  @Override
  public void instantiate(
      final LinkedList<QueryPlan> planQ,
      final LinkedList<SubQuery> subQueryQ,
      final ConstructArgs args)
      throws DbException {
    QueryPlan task = planQ.peekFirst();
    Verify.verify(
        task == this,
        "this Fragment %s should be the first object on the queue, not %s!",
        this,
        task);
    planQ.removeFirst();

    Map<Integer, SubQueryPlan> allPlans;
    try {
      allPlans = QueryConstruct.instantiate(fragments, args);
    } catch (CatalogException e) {
      throw new DbException("Error instantiating JsonSubQuery", e);
    }
    SubQueryPlan serverPlan = allPlans.get(MyriaConstants.MASTER_ID);
    Map<Integer, SubQueryPlan> workerPlans;
    if (serverPlan != null) {
      workerPlans = new HashMap<>();
      for (Map.Entry<Integer, SubQueryPlan> entry : allPlans.entrySet()) {
        if (entry.getKey() != MyriaConstants.MASTER_ID) {
          workerPlans.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      workerPlans = allPlans;
      /* Create the empty server plan. TODO why do we need this? */
      serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    }

    String planEncoding = null;
    try {
      planEncoding = MyriaJsonMapperProvider.getMapper().writeValueAsString(fragments);
    } catch (JsonProcessingException e) {
      LOGGER.error("Unable to encode JsonSubQuery fragments", e);
    }
    subQueryQ.addFirst(new SubQuery(null, serverPlan, workerPlans, planEncoding));
  }

  @Override
  public void reset() {
    /* Do nothing. */
  }
}
