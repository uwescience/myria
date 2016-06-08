package edu.washington.escience.myria.parallel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * A {@link LocalSubQuery} running at the Master. Currently, a {@link MasterSubQuery} can only have a single
 * {@link LocalFragment}.
 */
public class MasterSubQuery extends LocalSubQuery {

  /**
   * Record worker execution info.
   */
  private class WorkerExecutionInfo {
    /**
     * @param workerID owner worker id of the {@link LocalSubQuery}.
     * @param workerPlan the query plan of the {@link LocalSubQuery}.
     */
    WorkerExecutionInfo(final int workerID, final SubQueryPlan workerPlan) {
      this.workerPlan = workerPlan;
      workerReceiveSubQuery = new LocalSubQueryFuture(MasterSubQuery.this, false);
      workerReceiveSubQuery.addListener(
          new LocalSubQueryFutureListener() {

            @Override
            public void operationComplete(final LocalSubQueryFuture future) throws Exception {
              int total = workerExecutionInfo.size();
              int current = nowReceived.incrementAndGet();
              if (current >= total) {
                workerReceiveFuture.setSuccess();
              }
            }
          });
      workerCompleteQuery = new LocalSubQueryFuture(MasterSubQuery.this, false);
      workerCompleteQuery.addListener(
          new LocalSubQueryFutureListener() {

            @Override
            public void operationComplete(final LocalSubQueryFuture future) throws Exception {
              int total = workerExecutionInfo.size();
              int current = nowCompleted.incrementAndGet();
              if (!future.isSuccess()) {
                Throwable cause = future.getCause();
                if (!(cause instanceof QueryKilledException)) {
                  // Only record non-killed exceptions
                  if (getFTMode().equals(FTMode.NONE)) {
                    failedWorkerLocalSubQueries.put(workerID, cause);
                    // if any worker fails because of some exception, kill the query.
                    kill();
                    /* Record the reason for failure. */
                    if (cause != null) {
                      message =
                          MoreObjects.firstNonNull(
                              message, "Error in worker#" + workerID + ", " + cause.toString());
                    }
                  } else if (getFTMode().equals(FTMode.ABANDON)) {
                    LOGGER.debug(
                        "(Abandon) ignoring failed subquery future on subquery #{}",
                        getSubQueryId());
                    // do nothing
                  } else if (getFTMode().equals(FTMode.REJOIN)) {
                    LOGGER.debug(
                        "(Rejoin) ignoring failed subquery future on subquery #{}",
                        getSubQueryId());
                    // do nothing
                  }
                }
              }
              if (current >= total) {
                getExecutionStatistics().markEnd();
                LOGGER.info(
                    "Query #{} executed for {}",
                    getSubQueryId(),
                    DateTimeUtils.nanoElapseToHumanReadable(
                        getExecutionStatistics().getQueryExecutionElapse()));

                if (!killed.get() && failedWorkerLocalSubQueries.isEmpty()) {
                  queryExecutionFuture.setSuccess();
                } else {
                  if (failedWorkerLocalSubQueries.isEmpty()) {
                    // query gets killed.
                    queryExecutionFuture.setFailure(new QueryKilledException());
                  } else {
                    DbException composedException = null;
                    for (Entry<Integer, Throwable> workerIDCause :
                        failedWorkerLocalSubQueries.entrySet()) {
                      int failedWorkerID = workerIDCause.getKey();
                      Throwable cause = workerIDCause.getValue();
                      if (composedException == null) {
                        composedException =
                            new DbException(
                                "Query #" + getSubQueryId() + " failed: " + cause.getMessage());
                      }
                      if (!(cause instanceof QueryKilledException)) {
                        // Only record non-killed exceptions
                        DbException workerException =
                            new DbException(
                                "Worker #" + failedWorkerID + " failed: " + cause.getMessage(),
                                cause);
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
     * The {@link SubQueryPlan} that's assigned to the master.
     */
    private final SubQueryPlan workerPlan;

    /**
     * The future denoting the status of {@link SubQuery} dispatching to the workers.
     */
    private final LocalSubQueryFuture workerReceiveSubQuery;

    /**
     * The future denoting the status of {@link SubQuery} execution on the worker.
     */
    private final LocalSubQueryFuture workerCompleteQuery;
  }

  /**
   * logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterSubQuery.class);

  /**
   * The actual physical plan for the master {@link LocalSubQuery}.
   */
  private volatile LocalFragment fragment;

  /**
   * The owner master.
   */
  private final Server master;

  /**
   * The data structure denoting the query dispatching/execution status of each worker.
   */
  private final ConcurrentHashMap<Integer, WorkerExecutionInfo> workerExecutionInfo;

  /**
   * the number of workers currently received the query.
   */
  private final AtomicInteger nowReceived = new AtomicInteger();

  /**
   * The number of workers currently completed the query.
   */
  private final AtomicInteger nowCompleted = new AtomicInteger();

  /**
   * The future object denoting the worker receive query plan operation.
   */
  private final LocalSubQueryFuture workerReceiveFuture = new LocalSubQueryFuture(this, false);

  /**
   * The future object denoting the query execution progress.
   */
  private final LocalSubQueryFuture queryExecutionFuture = new LocalSubQueryFuture(this, false);

  /**
   * Current alive worker set.
   */
  private final Set<Integer> missingWorkers;

  /**
   * record all failed {@link LocalSubQuery}s.
   */
  private final ConcurrentHashMap<Integer, Throwable> failedWorkerLocalSubQueries =
      new ConcurrentHashMap<>();

  /**
   * The future listener for processing the complete events of the execution of the master fragment.
   */
  private final LocalFragmentFutureListener fragmentExecutionListener =
      new LocalFragmentFutureListener() {

        @Override
        public void operationComplete(final LocalFragmentFuture future) throws Exception {
          if (future.isSuccess()) {
            workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setSuccess();
          } else {
            workerExecutionInfo
                .get(MyriaConstants.MASTER_ID)
                .workerCompleteQuery
                .setFailure(future.getCause());
          }
        }
      };

  /**
   * Callback when a query plan is received by a worker.
   *
   * @param workerID the workerID
   */
  final void queryReceivedByWorker(final int workerID) {
    WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    LOGGER.debug("Worker #{} received query #{}", workerID, getSubQueryId());
    if (wei.workerReceiveSubQuery.isSuccess()) {
      /* a recovery worker */
      master
          .getIPCConnectionPool()
          .sendShortMessage(workerID, IPCUtils.startQueryTM(getSubQueryId()));
      for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
        if (e.getKey() == workerID) {
          /* the new worker doesn't need to start recovery tasks */
          continue;
        }
        if (!e.getValue().workerCompleteQuery.isDone() && e.getKey() != MyriaConstants.MASTER_ID) {
          master
              .getIPCConnectionPool()
              .sendShortMessage(e.getKey(), IPCUtils.recoverQueryTM(getSubQueryId(), workerID));
        }
      }
    } else {
      wei.workerReceiveSubQuery.setSuccess();
    }
  }

  /**
   * @return worker plans.
   */
  final Map<Integer, SubQueryPlan> getWorkerPlans() {
    Map<Integer, SubQueryPlan> result =
        new HashMap<Integer, SubQueryPlan>(workerExecutionInfo.size());
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() != MyriaConstants.MASTER_ID) {
        result.put(e.getKey(), e.getValue().workerPlan);
      }
    }
    return result;
  }

  /**
   * @return query future for the worker receiving query action.
   */
  final LocalSubQueryFuture getWorkerReceiveFuture() {
    return workerReceiveFuture;
  }

  @Override
  public final LocalSubQueryFuture getExecutionFuture() {
    return queryExecutionFuture;
  }

  /**
   * @return the set of workers get assigned to run the query.
   */
  final Set<Integer> getWorkerAssigned() {
    Set<Integer> s = new HashSet<Integer>(workerExecutionInfo.keySet());
    s.remove(MyriaConstants.MASTER_ID);
    return s;
  }

  /**
   * @return the set of workers who havn't finished their execution of the query.
   */
  final Set<Integer> getWorkersUnfinished() {
    Set<Integer> result = new HashSet<>();
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() == MyriaConstants.MASTER_ID) {
        continue;
      }
      LocalSubQueryFuture workerExecutionFuture = e.getValue().workerCompleteQuery;
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
   */
  final void workerComplete(final int workerID) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn(
          "Got a QUERY_COMPLETE (succeed) message from worker {} who is not assigned to query #{}",
          workerID,
          getSubQueryId());
      return;
    }
    LOGGER.debug("Received query complete (succeed) message from worker: {}", workerID);
    wei.workerCompleteQuery.setSuccess();
  }

  /**
   * Callback when a worker fails in executing its part of the query.
   *
   * @param workerID the workerID
   * @param cause the cause of the failure
   */
  final void workerFail(final int workerID, final Throwable cause) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn(
          "Got a QUERY_COMPLETE (fail) message from worker {} who is not assigned to query #{}",
          workerID,
          getSubQueryId());
      return;
    }

    LOGGER.info(
        "Received query complete (fail) message from worker: {}, cause: {}", workerID, cause);
    if (getFTMode().equals(FTMode.REJOIN) && cause.toString().endsWith("LostHeartbeatException")) {
      /* for rejoin, don't set it to be completed since this worker is expected to be launched again. */
      return;
    }
    wei.workerCompleteQuery.setFailure(cause);
  }

  /**
   * @param subQuery the {@link SubQuery} to be executed.
   * @param master the master on which the {@link SubQuery} is running.
   */
  public MasterSubQuery(final SubQuery subQuery, final Server master) {
    super(
        Preconditions.checkNotNull(
            Preconditions.checkNotNull(subQuery, "subQuery").getSubQueryId(), "subQueryId"),
        subQuery.getMasterPlan().getFTMode(),
        subQuery.getMasterPlan().getProfilingMode());
    Preconditions.checkNotNull(subQuery, "subQuery");
    SubQueryPlan masterPlan = subQuery.getMasterPlan();
    Map<Integer, SubQueryPlan> workerPlans = subQuery.getWorkerPlans();
    RootOperator root = masterPlan.getRootOps().get(0);
    this.master = master;
    workerExecutionInfo = new ConcurrentHashMap<Integer, WorkerExecutionInfo>(workerPlans.size());

    for (Entry<Integer, SubQueryPlan> workerInfo : workerPlans.entrySet()) {
      workerExecutionInfo.put(
          workerInfo.getKey(), new WorkerExecutionInfo(workerInfo.getKey(), workerInfo.getValue()));
    }
    WorkerExecutionInfo masterPart = new WorkerExecutionInfo(MyriaConstants.MASTER_ID, masterPlan);
    workerExecutionInfo.put(MyriaConstants.MASTER_ID, masterPart);

    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    fragment =
        new LocalFragment(
            this.master.getIPCConnectionPool(), this, root, master.getQueryExecutor());
    fragment.getExecutionFuture().addListener(fragmentExecutionListener);
  }

  @Override
  public final String toString() {
    return getFragments().toString();
  }

  @Override
  public final void startExecution() {
    LOGGER.info("Starting execution for query #{}", getSubQueryId());
    getExecutionStatistics().markStart();
    fragment.start();
  }

  @Override
  public final void kill() {
    if (!killed.compareAndSet(false, true)) {
      return;
    }
    fragment.kill();
    Set<Integer> workers = getWorkersUnfinished();
    ChannelFuture[] cfs = new ChannelFuture[workers.size()];
    int i = 0;
    DefaultChannelGroup cg = new DefaultChannelGroup();
    for (Integer workerID : workers) {
      cfs[i] =
          master
              .getIPCConnectionPool()
              .sendShortMessage(workerID, IPCUtils.killQueryTM(getSubQueryId()));
      cg.add(cfs[i].getChannel());
      i++;
    }
    DefaultChannelGroupFuture f = new DefaultChannelGroupFuture(cg, Arrays.asList(cfs));
    f.awaitUninterruptibly();
    if (!f.isCompleteSuccess()) {
      LOGGER.error("Send kill query message to workers failed.");
    }
  }

  @Override
  public final void init() {
    ImmutableMap.Builder<String, Object> queryExecEnvVars = ImmutableMap.builder();
    queryExecEnvVars.put(MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getSubQueryId().getQueryId());
    fragment.init(queryExecEnvVars.putAll(master.getExecEnvVars()).build());
  }

  /**
   * If the query has been asked to get killed (the kill event may not have completed).
   */
  private final AtomicBoolean killed = new AtomicBoolean(false);

  /**
   * Describes the cause of the query's death.
   */
  private volatile String message = null;

  /**
   * @return If the query has been asked to get killed (the kill event may not have completed).
   */
  public final boolean isKilled() {
    return killed.get();
  }

  @Override
  public Set<Integer> getMissingWorkers() {
    return missingWorkers;
  }

  /**
   * @return the message describing the cause of the query's death. Nullable.
   */
  public String getMessage() {
    return message;
  }

  @Override
  public Set<LocalFragment> getFragments() {
    return ImmutableSet.of(fragment);
  }
}
