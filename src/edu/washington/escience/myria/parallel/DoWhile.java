package edu.washington.escience.myria.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;

/**
 * A {@link QueryPlan} node that signifies running each of its children serially.
 */
public final class DoWhile extends QueryPlan {
  /** The query plans in the body. */
  private final List<QueryPlan> body;
  /** The relation to be scanned for the while loop continuation condition. */
  private final String condition;
  /** Whether this while loop has ever been run. */
  private boolean hasRun;

  /**
   * Construct a {@link QueryPlan} that runs the body tasks in sequence, and loops if the singleton boolean relation
   * specified by {@code condition} is {@code true}.
   *
   * @param body the tasks to be run.
   * @param condition the relation to be scanned for the while loop continuation condition. Must be a singleton boolean
   *          relation.
   */
  public DoWhile(final List<? extends QueryPlan> body, final String condition) {
    this.body = ImmutableList.copyOf(Objects.requireNonNull(body, "body"));
    this.condition = Objects.requireNonNull(condition, "condition");
    hasRun = false;
  }

  /**
   * {@inheritdoc}
   *
   * Instantiating a {@link DoWhile} is a somewhat complex operation.
   *
   * 1. Every {@link DoWhile} tracks whether or not the loop has been entered in {@link #hasRun}. For all times but the
   * first, the {@link #condition} is checked and, if {@code false}, the {@link DoWhile} is simply removed from the
   * {@code planQ} and nothing is added. This signifies the loop termination.
   *
   * 2. When we want to execute the loop, we want to do two things: run the body, then gather the value of the
   * termination condition and store it at the {@link Server} in a query global variable. Thus we want to add the
   * sequence of operations {@code body, then [gatherCondition]} to the plan.
   *
   * @param planQ the queue of {@link QueryPlan} tasks
   * @param subQueryQ the queue of {@link SubQuery} tasks
   * @param args the {@link QueryConstruct#ConstructArgs} arguments needed to instantiate a query plan
   */
  @Override
  public void instantiate(
      final LinkedList<QueryPlan> planQ,
      final LinkedList<SubQuery> subQueryQ,
      final ConstructArgs args) {
    QueryPlan checkTask = planQ.peekFirst();
    Verify.verify(
        checkTask == this,
        "this %s should be the first object on the queue, not %s!",
        this,
        checkTask);
    if (hasRun) {
      Boolean b = (Boolean) args.getServer().getQueryGlobal(args.getQueryId(), condition);
      Preconditions.checkState(
          b != null,
          "Query %s: DoWhile Loop has run, but global variable for loop condition (%s) has not been set",
          condition);
      if (!b.booleanValue()) {
        planQ.removeFirst();
        return;
      }
      for (QueryPlan q : body) {
        q.reset();
      }
    }
    // Note that the setDoWhileCondition will actually end up being after the body in the planQ
    planQ.addFirst(QueryConstruct.setDoWhileCondition(condition));
    planQ.addAll(0, body);
    hasRun = true;
  }

  @Override
  public void reset() {
    hasRun = false;
    for (QueryPlan q : body) {
      q.reset();
    }
  }
}
