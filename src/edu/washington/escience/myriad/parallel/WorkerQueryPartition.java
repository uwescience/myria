package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.operator.RootOperator;

/**
 * A {@link WorkerQueryPartition} is a partition of a query plan at a single worker.
 * */
public class WorkerQueryPartition implements QueryPartition {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryPartition.class);

  /**
   * The query ID.
   * */
  private final long queryID;

  /**
   * The operators.
   * */
  private final RootOperator[] operators;

  /**
   * All tasks.
   * */
  private final ConcurrentHashMap<QuerySubTreeTask, Boolean> tasks;

  /**
   * Number of EOSed tasks.
   * */
  private final AtomicInteger numTaskEOS;

  /**
   * The owner {@link Worker}.
   * */
  private final Worker ownerWorker;

  /**
   * priority, currently no use.
   * */
  private volatile int priority;

  /**
   * Store the current pause future if the query is in pause, otherwise null.
   * */
  private final AtomicReference<QueryFuture> pauseFuture = new AtomicReference<QueryFuture>(null);

  /**
   * The future object denoting the query execution progress.
   * */
  private final QueryFuture queryKillFuture = new DefaultQueryFuture(this, false);

  // private final QueryFuture executionFuture = new DefaultQueryFuture(this, true);

  /**
   * @param operators the operators belonging to this query partition.
   * @param queryID the id of the query.
   * @param ownerWorker the worker on which this query partition is going to run
   * */
  public WorkerQueryPartition(final RootOperator[] operators, final long queryID, final Worker ownerWorker) {
    this.queryID = queryID;
    this.operators = operators;
    tasks = new ConcurrentHashMap<QuerySubTreeTask, Boolean>(operators.length);
    numTaskEOS = new AtomicInteger(0);
    this.ownerWorker = ownerWorker;
  }

  @Override
  public final long getQueryID() {
    return queryID;
  }

  @Override
  public final int compareTo(final QueryPartition o) {
    if (o == null) {
      return -1;
    }
    return priority - o.getPriority();
  }

  /**
   * @return the root operators belonging to this query partition.
   * */
  public final RootOperator[] getOperators() {
    return operators;
  }

  @Override
  public final void setPriority(final int priority) {
    this.priority = priority;
  }

  @Override
  public final String toString() {
    return Arrays.toString(operators) + ", priority:" + priority;
  }

  /**
   * set my execution tasks.
   * 
   * @param tasks my tasks.
   * */
  public final void setTasks(final ArrayList<QuerySubTreeTask> tasks) {
    for (QuerySubTreeTask t : tasks) {
      this.tasks.put(t, new Boolean(false));
    }
  }

  @Override
  public final void startNonBlockingExecution() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query : " + this + " start processing.");
    }
    for (QuerySubTreeTask t : tasks.keySet()) {
      t.init(ImmutableMap.copyOf(ownerWorker.getExecEnvVars()));
      t.nonBlockingExecute();
    }
  }

  /**
   * start blocking execution.
   */
  @Deprecated
  public final void startBlockingExecution() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query : " + this + " start processing.");
    }
    for (QuerySubTreeTask t : tasks.keySet()) {
      t.init(ImmutableMap.copyOf(ownerWorker.getExecEnvVars()));
      t.blockingExecute();
    }
  }

  /**
   * Callback when all tasks have finished.
   * */
  private void queryFinish() {
    for (QuerySubTreeTask t : tasks.keySet()) {
      t.cleanup();
    }
    ownerWorker.finishQuery(this);
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("query: " + this + " finished");
    }
  }

  @Override
  public final void taskFinish(final QuerySubTreeTask task) {
    if (!tasks.containsKey(task)) {
      LOGGER.error("Unknown task finish: {} ", task);
      return;
    }

    if (!tasks.replace(task, false, true)) {
      LOGGER.error("Duplicate task eos: {} ", task);
      return;
    }

    int currentNumEOS = numTaskEOS.incrementAndGet();

    LOGGER.debug("new EOS from task: {}. {} remain.", task, (tasks.size() - currentNumEOS));
    if (currentNumEOS >= tasks.size()) {
      // all tasks have been EOS
      queryFinish();
    }
  }

  @Override
  public final int getNumTaskEOS() {
    return numTaskEOS.get();
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  /**
   * Pause the worker query partition. If the query is currently paused, do nothing.
   * 
   * @return the future instance of the pause action. The future will be set as done if and only if all the tasks in
   *         this query have stopped execution. During a pause of the query, all call to this method returns the same
   *         future instance. Two pause calls when the query is not paused at either of the calls return two different
   *         instances.
   * */
  @Override
  public final QueryFuture pause() {
    final QueryFuture pauseF = new DefaultQueryFuture(this, true);
    while (!pauseFuture.compareAndSet(null, pauseF)) {
      QueryFuture current = pauseFuture.get();
      if (current != null) {
        // already paused by some other threads, do not do the actual pause
        return current;
      }
    }
    return pauseF;
  }

  /**
   * Resume the worker query partition.
   * 
   * @return the future instance of the resume action.
   * */
  @Override
  public final QueryFuture resume() {
    QueryFuture pf = pauseFuture.getAndSet(null);
    QueryFuture rf = new DefaultQueryFuture(this, true);

    if (pf == null) {
      // query is not in pause, return success directly.
      rf.setSuccess();
      return rf;
    }
    // TODO do the resume stuff
    return rf;
  }

  /**
   * Kill the worker query partition.
   * 
   * @return the future instance of the kill action.
   * */
  @Override
  public final QueryFuture kill() {
    for (Map.Entry<QuerySubTreeTask, Boolean> e : tasks.entrySet()) {
      QuerySubTreeTask t = e.getKey();
      t.kill();
    }
    return queryKillFuture;
  }

  @Override
  public final boolean isPaused() {
    return pauseFuture.get() != null;
  }

  @Override
  public final boolean isKilled() {
    return false;
  }

}
