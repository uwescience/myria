package edu.washington.escience.myria.parallel.meta;

import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.parallel.QueryTask;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

/**
 * A meta-task that runs a single plan fragment. Note that a {@link Fragment} cannot have a meta-task as its plan.
 */
public final class Fragment extends MetaTask {
  /** The server plan. */
  private final SingleQueryPlanWithArgs serverPlan;
  /** The worker plans. */
  private final Map<Integer, SingleQueryPlanWithArgs> workerPlans;

  /**
   * Construct a {@link MetaTask} that runs the given plan fragment.
   * 
   * @param serverPlan the part of this fragment executed at the server
   * @param workerPlans the part of this fragment executed at each worker
   */
  public Fragment(final SingleQueryPlanWithArgs serverPlan, final Map<Integer, SingleQueryPlanWithArgs> workerPlans) {
    this.serverPlan = Objects.requireNonNull(serverPlan, "serverPlan");
    this.workerPlans = ImmutableMap.copyOf(Objects.requireNonNull(workerPlans, "workerPlans"));
  }

  /**
   * @return the worker plans
   */
  public Map<Integer, SingleQueryPlanWithArgs> getWorkerPlans() {
    return workerPlans;
  }

  /**
   * @return the server plan
   */
  public SingleQueryPlanWithArgs getServerPlan() {
    return serverPlan;
  }

  @Override
  public void instantiate(final LinkedList<MetaTask> metaQ, final LinkedList<QueryTask> taskQ, final Server server) {
    MetaTask task = metaQ.peekFirst();
    Verify.verify(task == this, "this Fragment %s should be the first object on the queue, not %s!", this, task);
    metaQ.removeFirst();
    taskQ.addFirst(new QueryTask(serverPlan, workerPlans));
  }
}
