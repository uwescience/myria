package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.operator.RootOperator;

/**
 * A {@link MasterQueryPartition} is the partition of a query plan at the Master side. Currently, a master query
 * partition can only have a single task.
 * 
 * */
public class MasterQueryPartition implements QueryPartition {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterQueryPartition.class);

  /**
   * The query ID.
   * */
  private final long queryID;

  /**
   * The execution task for the master side query partition.
   * */
  private volatile QuerySubTreeTask rootTask;

  /**
   * The root operator of the master query partition.
   * */
  final RootOperator root;

  /**
   * The worker plans of the owner query of this master query partition.
   * */
  final ConcurrentHashMap<Integer, RootOperator[]> workerPlans;

  /**
   * If the root is EOS.
   * */
  private volatile boolean rootTaskEOS = false;

  /**
   * The owner master.
   * */
  private final Server master;

  /**
   * The priority.
   * */
  private volatile int priority;

  /**
   * The workers who have received their part of the query plan.
   * */
  final BitSet workersReceivedQuery;

  /**
   * The workers who have completed their part of the query plan.
   * */
  final BitSet workersCompleteQuery;

  /**
   * The workers get assigned to compute the query. WorkerID -> Worker Index.
   * */
  final ConcurrentHashMap<Integer, Integer> workersAssigned;

  /**
   * The future object denoting the worker receive query plan operation.
   * */
  final QueryFuture workerReceiveFuture = new DefaultQueryFuture(this, true);

  /**
   * The future object denoting the query execution progress.
   * */
  final QueryFuture queryExecutionFuture = new DefaultQueryFuture(this, true);

  /**
   * Callback when a query plan is received by a worker.
   * 
   * @param workerID the workerID
   * */
  final void queryReceivedByWorker(final int workerID) {
    final int workerIdx = workersAssigned.get(workerID);
    workersReceivedQuery.set(workerIdx);
    workerReceiveFuture.setProgress(1, workersReceivedQuery.cardinality(), workersAssigned.size());
    if (workersReceivedQuery.cardinality() >= workersAssigned.size()) {
      workerReceiveFuture.setSuccess();
    }
  }

  /**
   * Callback when a worker completes its part of the query.
   * 
   * @param workerID the workerID
   * */
  final void workerComplete(final int workerID) {
    final Integer workerIdx = workersAssigned.get(workerID);
    if (workerIdx == null) {
      LOGGER.warn("Got a QUERY_COMPLETE message from worker " + workerID + " who is not assigned to query" + queryID);
      return;
    }
    workersCompleteQuery.set(workerIdx);
    queryExecutionFuture.setProgress(1, workersCompleteQuery.cardinality(), workersAssigned.size());
    if (workersCompleteQuery.cardinality() >= workersAssigned.size() && rootTaskEOS) {
      queryExecutionFuture.setSuccess();
    }
  }

  public MasterQueryPartition(final RootOperator rootOp, final Map<Integer, RootOperator[]> workerPlans,
      final long queryID, final Server master) {
    root = rootOp;
    this.queryID = queryID;
    this.master = master;
    workersAssigned = new ConcurrentHashMap<Integer, Integer>(workerPlans.size());
    workersReceivedQuery = new BitSet(workerPlans.size());

    // the master part of the query plan always get index of 0
    int idx = 1;
    for (Integer workerID : workerPlans.keySet()) {
      workersAssigned.put(workerID, idx++);
    }
    workersCompleteQuery = new BitSet(workerPlans.size());
    this.workerPlans = new ConcurrentHashMap<Integer, RootOperator[]>(workerPlans);
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

  @Override
  public final void setPriority(final int priority) {
    this.priority = priority;
  }

  @Override
  public final String toString() {
    return rootTask + ", priority:" + priority;
  }

  public final void setRootTask(final QuerySubTreeTask rootTask) {
    this.rootTask = rootTask;
  }

  @Override
  public final void startNonBlockingExecution() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query : " + this + " start processing.");
    }
    rootTask.init(ImmutableMap.copyOf(master.execEnvVars));
    rootTask.nonBlockingExecute();
  }

  @Deprecated
  public final void startBlockingExecution() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query : " + this + " start processing.");
    }
    rootTask.init(ImmutableMap.copyOf(master.execEnvVars));
    rootTask.blockingExecute();
  }

  /**
   * Callback when all tasks have finished.
   * */
  private void queryFinish() {
    rootTask.cleanup();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("query: " + this + " finished");
    }
    rootTaskEOS = true;
    if (workersCompleteQuery.cardinality() >= workersAssigned.size()) {
      queryExecutionFuture.setSuccess();
    }
  }

  @Override
  public final void taskFinish(final QuerySubTreeTask task) {
    if (rootTaskEOS) {
      LOGGER.error("Duplicate task eos: {} ", task);
      return;
    }
    queryFinish();
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  @Override
  public final int getNumTaskEOS() {
    return rootTaskEOS ? 1 : 0;
  }

}
