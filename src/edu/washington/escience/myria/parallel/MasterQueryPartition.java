package edu.washington.escience.myria.parallel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * A {@link MasterQueryPartition} is the partition of a query plan at the Master side. Currently, a master query
 * partition can only have a single task.
 * 
 * */
public class MasterQueryPartition implements QueryPartition {

  /**
   * Record worker execution info.
   * */
  private class WorkerExecutionInfo {
    /**
     * @param workerID owner worker id of the partition.
     * @param workerPlan the query plan of this partition.
     * */
    WorkerExecutionInfo(final int workerID, final SingleQueryPlanWithArgs workerPlan) {
      this.workerID = workerID;
      this.workerPlan = workerPlan;
      workerReceiveQuery = new DefaultQueryFuture(MasterQueryPartition.this, false);
      workerReceiveQuery.addListener(new QueryFutureListener() {

        @Override
        public void operationComplete(final QueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowReceived.incrementAndGet();
          workerReceiveFuture.setProgress(1, current, workerExecutionInfo.size());
          if (current >= total) {
            workerReceiveFuture.setSuccess();
          }
        }
      });
      workerCompleteQuery = new DefaultQueryFuture(MasterQueryPartition.this, false);
      workerCompleteQuery.addListener(new QueryFutureListener() {

        @Override
        public void operationComplete(final QueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowCompleted.incrementAndGet();
          queryExecutionFuture.setProgress(1, current, workerExecutionInfo.size());
          if (!future.isSuccess()) {
            if (!(future.getCause() instanceof QueryKilledException)) {
              // Only record non-killed exceptions
              if (ftMode.equals(FTMODE.none)) {
                failedQueryPartitions.put(workerID, future.getCause());
                // if any worker fails because of some exception, kill the query.
                kill();
              } else if (ftMode.equals(FTMODE.abandon)) {
                // do nothing
              } else if (ftMode.equals(FTMODE.rejoin)) {
                // do nothing
              }
            }
          }
          if (current >= total) {
            queryStatistics.markQueryEnd();
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info("Query #" + queryID + " executed for "
                  + DateTimeUtils.nanoElapseToHumanReadable(queryStatistics.getQueryExecutionElapse()));
            }

            if (!killed && failedQueryPartitions.isEmpty()) {
              queryExecutionFuture.setSuccess();
            } else {
              if (failedQueryPartitions.isEmpty()) {
                // query gets killed.
                queryExecutionFuture.setFailure(new QueryKilledException());
              } else {
                DbException composedException =
                    new DbException("Query #" + future.getQuery().getQueryID() + " failed.");
                for (Entry<Integer, Throwable> workerIDCause : failedQueryPartitions.entrySet()) {
                  int failedWorkerID = workerIDCause.getKey();
                  Throwable cause = workerIDCause.getValue();
                  if (!(cause instanceof QueryKilledException)) {
                    // Only record non-killed exceptoins
                    DbException workerException =
                        new DbException("Worker #" + failedWorkerID + " failed: " + cause.getMessage(), cause);
                    workerException.setStackTrace(cause.getStackTrace());
                    for (Throwable sup : cause.getSuppressed()) {
                      workerException.addSuppressed(sup);
                    }
                    composedException.addSuppressed(workerException);
                  }
                }
                queryExecutionFuture.setFailure(composedException);
              }
            }
          }
        }
      });
    }

    /**
     * The worker (maybe the master) who is executing the query partition.
     * */
    @SuppressWarnings("unused")
    private final int workerID;

    /**
     * The query plan that's assigned to the worker.
     * */
    private final SingleQueryPlanWithArgs workerPlan;

    /**
     * The future denoting the status of query partition dispatching to the worker event.
     * */
    private final DefaultQueryFuture workerReceiveQuery;

    /**
     * The future denoting the status of query partition execution on the worker.
     * */
    private final DefaultQueryFuture workerCompleteQuery;
  }

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

  /***
   * Statistics of this query partition.
   */
  private final QueryExecutionStatistics queryStatistics = new QueryExecutionStatistics();

  /**
   * The root operator of the master query partition.
   * */
  private final RootOperator root;

  /**
   * The owner master.
   * */
  private final Server master;

  /**
   * The FT mode.
   * */
  private final FTMODE ftMode;

  /**
   * The priority.
   * */
  private volatile int priority;

  /**
   * The data structure denoting the query dispatching/execution status of each worker.
   * */
  private final ConcurrentHashMap<Integer, WorkerExecutionInfo> workerExecutionInfo;

  /**
   * the number of workers currently received the query.
   * */
  private final AtomicInteger nowReceived = new AtomicInteger();

  /**
   * The number of workers currently completed the query.
   * */
  private final AtomicInteger nowCompleted = new AtomicInteger();

  /**
   * The future object denoting the worker receive query plan operation.
   * */
  private final DefaultQueryFuture workerReceiveFuture = new DefaultQueryFuture(this, false);

  /**
   * The future object denoting the query execution progress.
   * */
  private final DefaultQueryFuture queryExecutionFuture = new DefaultQueryFuture(this, false);

  /**
   * Current alive worker set.
   * */
  private final Set<Integer> missingWorkers;

  /**
   * Store the current pause future if the query is in pause, otherwise null.
   * */
  private final AtomicReference<QueryFuture> pauseFuture = new AtomicReference<QueryFuture>(null);

  /**
   * record all failed query partitions.
   * */
  private final ConcurrentHashMap<Integer, Throwable> failedQueryPartitions =
      new ConcurrentHashMap<Integer, Throwable>();

  /**
   * The future listener for processing the complete events of the execution of the master task.
   * */
  private final TaskFutureListener taskExecutionListener = new TaskFutureListener() {

    @Override
    public void operationComplete(final TaskFuture future) throws Exception {
      if (future.isSuccess()) {
        if (root instanceof SinkRoot) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info(" Root task {} EOS. Num output tuple: {}", rootTask, ((SinkRoot) root).getCount());
          }
        }
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setSuccess();
      } else {
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setFailure(future.getCause());
      }
    }

  };

  /**
   * Callback when a query plan is received by a worker.
   * 
   * @param workerID the workerID
   * */
  final void queryReceivedByWorker(final int workerID) {
    WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Worker #{} received query#{}", workerID, queryID);
    }
    wei.workerReceiveQuery.setSuccess();
  }

  /**
   * @return worker plans.
   * */
  final Map<Integer, SingleQueryPlanWithArgs> getWorkerPlans() {
    Map<Integer, SingleQueryPlanWithArgs> result =
        new HashMap<Integer, SingleQueryPlanWithArgs>(workerExecutionInfo.size());
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() != MyriaConstants.MASTER_ID) {
        result.put(e.getKey(), e.getValue().workerPlan);
      }
    }
    return result;
  }

  /**
   * @return query future for the worker receiving query action.
   * */
  final QueryFuture getWorkerReceiveFuture() {
    return workerReceiveFuture;
  }

  /**
   * @return query future for the worker receiving query action.
   * */
  final QueryFuture getQueryExecutionFuture() {
    return queryExecutionFuture;
  }

  /**
   * @return my root operator.
   * */
  final RootOperator getRootOperator() {
    return root;
  }

  /**
   * @return the set of workers get assigned to run the query.
   * */
  final Set<Integer> getWorkerAssigned() {
    Set<Integer> s = new HashSet<Integer>(workerExecutionInfo.keySet());
    s.remove(MyriaConstants.MASTER_ID);
    return s;
  }

  /**
   * @return the set of workers who havn't finished their execution of the query.
   * */
  final Set<Integer> getWorkersUnfinished() {
    Set<Integer> result = new HashSet<Integer>();
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() == MyriaConstants.MASTER_ID) {
        continue;
      }
      QueryFuture workerExecutionFuture = e.getValue().workerCompleteQuery;
      if (!workerExecutionFuture.isDone()) {
        result.add(e.getKey());
      }
    }
    return result;
  }

  /**
   * Callback when a worker completes its part of the query.
   * 
   * @param workerID the workerID
   * */
  final void workerComplete(final int workerID) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (succeed) message from worker " + workerID + " who is not assigned to query"
          + queryID);
      return;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received query complete (succeed) message from worker: {}", workerID);
    }

    wei.workerCompleteQuery.setSuccess();
  }

  /**
   * Callback when a worker fails in executing its part of the query.
   * 
   * @param workerID the workerID
   * @param cause the cause of the failure
   * */
  final void workerFail(final int workerID, final Throwable cause) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (fail) message from worker " + workerID + " who is not assigned to query"
          + queryID);
      return;
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Received query complete (fail) message from worker: {}, cause: {}", workerID, cause.toString());
    }
    if (ftMode.equals(FTMODE.rejoin) && cause.toString().endsWith("LostHeartbeatException")) {
      /* for rejoin, don't set it to be completed since this worker is expected to be launched again. */
      return;
    }
    wei.workerCompleteQuery.setFailure(cause);
  }

  /**
   * @param masterPlan the master plan.
   * @param workerPlans the worker plans.
   * @param queryID queryID.
   * @param master the master on which the query partition is running.
   * */
  public MasterQueryPartition(final SingleQueryPlanWithArgs masterPlan,
      final Map<Integer, SingleQueryPlanWithArgs> workerPlans, final long queryID, final Server master) {
    root = masterPlan.getRootOps().get(0);
    this.queryID = queryID;
    this.master = master;
    ftMode = masterPlan.getFTMode();
    workerExecutionInfo = new ConcurrentHashMap<Integer, WorkerExecutionInfo>(workerPlans.size());

    for (Entry<Integer, SingleQueryPlanWithArgs> workerInfo : workerPlans.entrySet()) {
      workerExecutionInfo.put(workerInfo.getKey(), new WorkerExecutionInfo(workerInfo.getKey(), workerInfo.getValue()));
    }
    WorkerExecutionInfo masterPart = new WorkerExecutionInfo(MyriaConstants.MASTER_ID, masterPlan);
    workerExecutionInfo.put(MyriaConstants.MASTER_ID, masterPart);

    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    rootTask = new QuerySubTreeTask(MyriaConstants.MASTER_ID, this, root, master.getQueryExecutor());
    rootTask.getExecutionFuture().addListener(taskExecutionListener);
    HashSet<Consumer> consumerSet = new HashSet<Consumer>();
    consumerSet.addAll(rootTask.getInputChannels().values());

    for (final Consumer operator : consumerSet) {
      FlowControlBagInputBuffer<TupleBatch> inputBuffer =
          new FlowControlBagInputBuffer<TupleBatch>(this.master.getIPCConnectionPool(), operator
              .getInputChannelIDs(this.master.getIPCConnectionPool().getMyIPCID()), master.getInputBufferCapacity(),
              master.getInputBufferRecoverTrigger(), this.master.getIPCConnectionPool());
      operator.setInputBuffer(inputBuffer);
      inputBuffer.addListener(FlowControlBagInputBuffer.NEW_INPUT_DATA, new IPCEventListener() {

        @Override
        public void triggered(final IPCEvent event) {
          rootTask.notifyNewInput();
        }
      });
    }
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

  @Override
  public final void startExecution() {
    queryStatistics.markQueryStart();
    rootTask.execute();
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  /**
   * Pause the master query partition.
   * 
   * @return the future instance of the pause action. The future will be set as done if and only if all the tasks in
   *         this query have stopped execution. During a pause of the query, all call to this method returns the same
   *         future instance. Two pause calls when the query is not paused at either of the calls return two different
   *         instances.
   */
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
   * Resume the master query partition.
   * 
   * @return the future instance of the resume action.
   * */
  @Override
  public final QueryFuture resume() {
    QueryFuture pf = pauseFuture.getAndSet(null);
    DefaultQueryFuture rf = new DefaultQueryFuture(this, true);

    if (pf == null) {
      rf.setSuccess();
      return rf;
    }

    // TODO do the resume stuff
    return rf;
  }

  /**
   * Kill the master query partition.
   * 
   * */
  @Override
  public final void kill() {
    if (killed) {
      return;
    }
    killed = true;
    rootTask.kill();
    Set<Integer> workers = getWorkersUnfinished();
    ChannelFuture[] cfs = new ChannelFuture[workers.size()];
    int i = 0;
    DefaultChannelGroup cg = new DefaultChannelGroup();
    for (Integer workerID : workers) {
      cfs[i] = master.getIPCConnectionPool().sendShortMessage(workerID, IPCUtils.killQueryTM(getQueryID()));
      cg.add(cfs[i].getChannel());
      i++;
    }
    DefaultChannelGroupFuture f = new DefaultChannelGroupFuture(cg, Arrays.asList(cfs));
    f.awaitUninterruptibly();
    if (!f.isCompleteSuccess()) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Send kill query message to workers failed.");
      }
    }
  }

  @Override
  public final boolean isPaused() {
    return pauseFuture.get() != null;
  }

  @Override
  public final void init() {
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    TaskResourceManager resourceManager =
        new TaskResourceManager(master.getIPCConnectionPool(), rootTask, master.getExecutionMode());
    rootTask.init(resourceManager, b.putAll(master.getExecEnvVars()).build());
  }

  @Override
  public final QueryExecutionStatistics getExecutionStatistics() {
    return queryStatistics;
  }

  /**
   * If the query has been asked to get killed (the kill event may not have completed).
   * */
  private volatile boolean killed = false;

  /**
   * @return If the query has been asked to get killed (the kill event may not have completed).
   * */
  public final boolean isKilled() {
    return killed;
  }

  @Override
  public FTMODE getFTMode() {
    return ftMode;
  }

  @Override
  public Set<Integer> getMissingWorkers() {
    return missingWorkers;
  }

  /**
   * when a REMOVE_WORKER message is received, give tasks another chance to decide if they are ready to generate
   * EOS/EOI.
   */
  public void triggerTasks() {
    rootTask.notifyNewInput();
  }

  /**
   * enable/disable output channels of the root(producer) of each task.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   * */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    rootTask.updateProducerChannels(workerId, enable);
  }
}
