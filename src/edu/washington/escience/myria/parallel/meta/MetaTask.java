package edu.washington.escience.myria.parallel.meta;

import java.util.LinkedList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.parallel.QueryTask;
import edu.washington.escience.myria.parallel.Server;

/**
 * A meta-task like a Parallel, a Sequence, or a DoWhile.
 */
public abstract class MetaTask {

  /**
   * Populate the given meta and task queues with the meta and query tasks that arise from unrolling this task.
   * 
   * @param metaQ the queue of meta tasks
   * @param taskQ the queue of query tasks
   * @param server the server on which the tasks will be executed
   * @throws DbException if there is an error instantiating the task
   */
  public abstract void instantiate(LinkedList<MetaTask> metaQ, LinkedList<QueryTask> taskQ, Server server)
      throws DbException;
}
