package edu.washington.escience.myria.parallel;

import java.util.LinkedList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;

/**
 * A node in the query plan tree like a Parallel, a Sequence, or a DoWhile. Also may represent the entire query plan
 * itself when it is the root of the tree.
 */
public abstract class QueryPlan {

  /**
   * Populate the given {@link QueryPlan} and {@link SubQuery} queues with the query and subquery tasks that arise from
   * unrolling this {@link QueryPlan}.
   *
   * @param planQ the queue of {@link QueryPlan} tasks
   * @param subQueryQ the queue of {@link SubQuery} tasks
   * @param args the {@link QueryConstruct#ConstructArgs} arguments needed to instantiate a query plan
   * @throws DbException if there is an error instantiating the next {@link SubQuery}
   */
  public abstract void instantiate(
      LinkedList<QueryPlan> planQ, LinkedList<SubQuery> subQueryQ, ConstructArgs args)
      throws DbException;

  /**
   * Reset this {@link QueryPlan} so that it can be executed again.
   */
  public abstract void reset();
}
