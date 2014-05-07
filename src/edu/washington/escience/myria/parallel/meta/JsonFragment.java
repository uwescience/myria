package edu.washington.escience.myria.parallel.meta;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Verify;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.QueryTask;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

/**
 * A meta-task that runs a single plan fragment. Note that a {@link JsonFragment} cannot have a meta-task as its plan.
 */
public final class JsonFragment extends MetaTask {
  /** The json query to be executed. */
  private final List<PlanFragmentEncoding> plans;

  /**
   * Construct a {@link MetaTask} that runs the given plan fragment. The plans will be instantiated using
   * {@link QueryConstruct#instantiate(List, edu.washington.escience.myria.parallel.Server)}.
   * 
   * @param plans the JSON query to be executed
   * @see QueryConstruct#instantiate(List, edu.washington.escience.myria.parallel.Server)
   */
  public JsonFragment(final List<PlanFragmentEncoding> plans) {
    this.plans = Objects.requireNonNull(plans, "plans");
  }

  @Override
  public void instantiate(final LinkedList<MetaTask> metaQ, final LinkedList<QueryTask> taskQ, final Server server)
      throws DbException {
    MetaTask task = metaQ.peekFirst();
    Verify.verify(task == this, "this Fragment %s should be the first object on the queue, not %s!", this, task);
    metaQ.removeFirst();

    Map<Integer, SingleQueryPlanWithArgs> allPlans;
    try {
      allPlans = QueryConstruct.instantiate(plans, server);
    } catch (CatalogException e) {
      throw new DbException("Error instantiating JsonFragment", e);
    }
    SingleQueryPlanWithArgs serverPlan = allPlans.get(MyriaConstants.MASTER_ID);
    Map<Integer, SingleQueryPlanWithArgs> workerPlans;
    if (serverPlan != null) {
      workerPlans = new HashMap<>();
      for (Map.Entry<Integer, SingleQueryPlanWithArgs> entry : allPlans.entrySet()) {
        if (entry.getKey() != MyriaConstants.MASTER_ID) {
          workerPlans.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      workerPlans = allPlans;
      /* Create the empty server plan. TODO why do we need this? */
      serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(new EOSSource()));
    }

    taskQ.addFirst(new QueryTask(serverPlan, workerPlans));
  }
}
