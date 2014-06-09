package edu.washington.escience.myria.parallel;

import java.util.LinkedList;

import edu.washington.escience.myria.DbException;

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
   * @param server the server on which the {@link SubQuery} tasks will be executed
   * @param queryId the query of which the instantiated plan will be a part.
   * @throws DbException if there is an error instantiating the next {@link SubQuery}
   */
  public abstract void instantiate(LinkedList<QueryPlan> planQ, LinkedList<SubQuery> subQueryQ, Server server,
      long queryId) throws DbException;
}
