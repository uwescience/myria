package edu.washington.escience.myria.parallel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;

/**
 * A {@link MasterQueryPartition} is the partition of a query plan at the Master side. Currently, a master query
 * partition can only have a single task.
 * 
 * */
public class MasterQueryPartition extends QueryPartitionBase {

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
          getExecutionFuture().setProgress(1, current, workerExecutionInfo.size());
          long queryID = getQueryID();
          FTMODE ftMode = getFTMode();
          if (!future.isSuccess()) {
            Throwable cause = future.getCause();
            if (!(cause instanceof QueryKilledException)) {
              // Only record non-killed exceptions
              if (ftMode.equals(FTMODE.none)) {
                failedQueryPartitions.put(workerID, cause);
                // if any worker fails because of some exception, kill the query.
                kill();
                /* Record the reason for failure. */
                if (cause != null) {
                  message = Objects.firstNonNull(message, cause.toString());
                }
              } else if (ftMode.equals(FTMODE.abandon)) {
                LOGGER.debug("(Abandon) ignoring failed query future on query #{}", queryID);
                // do nothing
              } else if (getFTMode().equals(FTMODE.rejoin)) {
                LOGGER.debug("(Rejoin) ignoring failed query future on query #{}", getQueryID());
                // do nothing
              }
            }
          }
          if (current >= total) {
            getExecutionStatistics().markQueryEnd();
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info("Query #" + queryID + " executed for "
                  + DateTimeUtils.nanoElapseToHumanReadable(getExecutionStatistics().getQueryExecutionElapse()));
            }

            if (!isKilled() && failedQueryPartitions.isEmpty()) {
              getExecutionFuture().setSuccess();
            } else {
              if (failedQueryPartitions.isEmpty()) {
                // query gets killed.
                getExecutionFuture().setFailure(new QueryKilledException());
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
                getExecutionFuture().setFailure(composedException);
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
   * The execution task for the master side query partition.
   * */
  private final QuerySubTreeTask rootTask;

  /**
   * The root operator of the master query partition.
   * */
  private final RootOperator root;

  /**
   * The owner master.
   * */
  private final Server master;

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
      LOGGER.debug("Worker #{} received query#{}", workerID, getQueryID());
    }
    if (wei.workerReceiveQuery.isSuccess()) {
      /* a recovery worker */
      master.getIPCConnectionPool().sendShortMessage(workerID, IPCUtils.startQueryTM(getQueryID()));
      for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
        if (e.getKey() == workerID) {
          /* the new worker doesn't need to start recovery tasks */
          continue;
        }
        if (!e.getValue().workerCompleteQuery.isDone() && e.getKey() != MyriaConstants.MASTER_ID) {
          master.getIPCConnectionPool().sendShortMessage(e.getKey(), IPCUtils.recoverQueryTM(getQueryID(), workerID));
        }
      }
    } else {
      wei.workerReceiveQuery.setSuccess();
    }
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
          + getQueryID());
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
          + getQueryID());
      return;
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Received query complete (fail) message from worker: {}, cause: {}", workerID, cause.toString());
    }
    if (cause instanceof LostHeartbeatException) {
      switch (getFTMode()) {
        case abandon:
          getMissingWorkers().add(workerID);
          updateProducerChannels(workerID, false);
          triggerTasks();
          wei.workerCompleteQuery.setSuccess();
          break;
        case rejoin:
          getMissingWorkers().add(workerID);
          updateProducerChannels(workerID, false);
          break;
        case none:
          wei.workerCompleteQuery.setFailure(cause);
          break;
      }
    } else {
      wei.workerCompleteQuery.setFailure(cause);
    }
  }

  @Override
  protected StreamInputBuffer<TupleBatch> getInputBuffer(final RootOperator root, final Consumer c) {
    return new FlowControlBagInputBuffer<TupleBatch>(getIPCPool(), c.getInputChannelIDs(getIPCPool().getMyIPCID()),
        master.getInputBufferCapacity(), master.getInputBufferRecoverTrigger(), getIPCPool());
  }

  /**
   * @param masterPlan the master plan.
   * @param workerPlans the worker plans.
   * @param queryID queryID.
   * @param master the master on which the query partition is running.
   * */
  public MasterQueryPartition(final SingleQueryPlanWithArgs masterPlan,
      final Map<Integer, SingleQueryPlanWithArgs> workerPlans, final long queryID, final Server master) {
    super(masterPlan, queryID, master.getIPCConnectionPool());
    root = masterPlan.getRootOps().get(0);
    this.master = master;
    createInitialTasks();
    rootTask = getTasks().iterator().next().getExecutionFuture().addListener(taskExecutionListener).getTask();
    workerExecutionInfo = new ConcurrentHashMap<Integer, WorkerExecutionInfo>(workerPlans.size());

    for (Entry<Integer, SingleQueryPlanWithArgs> workerInfo : workerPlans.entrySet()) {
      workerExecutionInfo.put(workerInfo.getKey(), new WorkerExecutionInfo(workerInfo.getKey(), workerInfo.getValue()));
    }
    WorkerExecutionInfo masterPart = new WorkerExecutionInfo(MyriaConstants.MASTER_ID, masterPlan);
    workerExecutionInfo.put(MyriaConstants.MASTER_ID, masterPart);
  }

  @Override
  public final void startExecution(final QueryFuture future) {
    getExecutionStatistics().markQueryStart();
    rootTask.execute();
  }

  /**
   * Pause the master query partition.
   * 
   * @param future the future instance of the pause action. The future will be set as done if and only if all the tasks
   *          in this query have stopped execution. During a pause of the query, all call to this method returns the
   *          same future instance. Two pause calls when the query is not paused at either of the calls return two
   *          different instances.
   */
  @Override
  public final void pause(final DefaultQueryFuture future) {
    // TODO implement actual pause.
  }

  @Override
  public final void resume(final DefaultQueryFuture future) {
    // TODO implement actual resume.
  }

  @Override
  protected final void kill(final DefaultQueryFuture future) {
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
  public final void init(final DefaultQueryFuture initFuture) {
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    TaskResourceManager resourceManager =
        new TaskResourceManager(master.getIPCConnectionPool(), rootTask, master.getExecutionMode(), master
            .getQueryExecutor().nextTaskExecutor(rootTask));
    rootTask.init(resourceManager, b.putAll(master.getExecEnvVars()).build()).addListener(
        new OperationFutureListener() {
          @Override
          public void operationComplete(final OperationFuture future) throws Exception {
            if (!future.isSuccess()) {
              initFuture.setFailure(future.getCause());
            } else {
              initFuture.setSuccess();
            }
          }
        });
  }

  /**
   * Describes the cause of the query's death.
   */
  private volatile String message = null;

  /**
   * @return the message describing the cause of the query's death. Nullable.
   */
  public String getMessage() {
    return message;
  }
}
