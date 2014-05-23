package edu.washington.escience.myria.parallel;

import java.util.LinkedList;

import edu.washington.escience.myria.DbException;

/**
 * A meta-task like a Parallel, a Sequence, or a DoWhile.
 */
public abstract class MetaTask {

  /**
   * Populate the given meta and subquery queues with the meta and subquery tasks that arise from unrolling this
   * {@link MetaTask}.
   * 
   * @param metaQ the queue of meta tasks
   * @param subQueryQ the queue of {@link SubQuery}s
   * @param server the server on which the {@link SubQuery}s will be executed
   * @throws DbException if there is an error instantiating the {@link SubQuery}
   */
  public abstract void instantiate(LinkedList<MetaTask> metaQ, LinkedList<SubQuery> subQueryQ, Server server)
      throws DbException;
}
