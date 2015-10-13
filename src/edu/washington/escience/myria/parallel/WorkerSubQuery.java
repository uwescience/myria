package edu.washington.escience.myria.parallel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingState;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.operator.network.RecoverProducer;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.concurrent.ErrorLoggingTimerTask;

/**
 * A {@link LocalSubQuery} running at a worker.
 */
public class WorkerSubQuery extends LocalSubQuery {

  /**
   * logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerSubQuery.class);

  /**
   * All {@link LocalFragment}s of this {@link WorkerSubQuery}.
   */
  private final Set<LocalFragment> fragments;

  /**
   * Number of finished {@link LocalFragment}s.
   */
  private final AtomicInteger numFinishedFragments;

  /**
   * The owner {@link Worker}.
   */
  private final Worker worker;

  /**
   * the future for the query's execution.
   */
  private final LocalSubQueryFuture executionFuture = new LocalSubQueryFuture(this, true);

  /**
   * record all failed {@link LocalFragment}s.
   */
  private final ConcurrentLinkedQueue<LocalFragment> failFragments = new ConcurrentLinkedQueue<>();

  /**
   * Current alive worker set.
   */
  private final Set<Integer> missingWorkers;

  /**
   * Record milliseconds so that we can normalize the time in {@link ProfilingLogger}.
   * 
   * TODO: why can't we put this in {@link ExecutionStatistics} and/or compute it from the nano
   * start time there?
   */
  private volatile long startMilliseconds = 0;

  /** Report resource usage at a fixed rate. Only enabled when the profiling mode has resource. */
  private Timer resourceReportTimer;

  /**
   * The future listener for processing the complete events of the execution of all the subquery's
   * fragments.
   */
  private final LocalFragmentFutureListener fragmentExecutionListener =
      new LocalFragmentFutureListener() {

        @Override
        public void operationComplete(final LocalFragmentFuture future) throws Exception {
          LocalFragment drivingFragment = future.getFragment();
          int currentNumFinished = numFinishedFragments.incrementAndGet();

          Throwable failureReason = future.getCause();
          if (!future.isSuccess()) {
            failFragments.add(drivingFragment);
            if (!(failureReason instanceof QueryKilledException)) {
              // The fragment is a failure, not killed.
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("got a failed fragment, root op = {} cause {}", drivingFragment
                    .getRootOp().getOpName(), failureReason);
              }
              for (LocalFragment t : fragments) {
                // kill all the other {@link Fragment}s.
                t.kill();
              }
            }
          }

          if (currentNumFinished >= fragments.size()) {
            getExecutionStatistics().markEnd();
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info("Query #{} executed for {}", getSubQueryId(), DateTimeUtils
                  .nanoElapseToHumanReadable(getExecutionStatistics().getQueryExecutionElapse()));
            }
            if (getProfilingMode().size() > 0) {
              try {
                if (resourceReportTimer != null) {
                  resourceReportTimer.cancel();
                }
                getWorker().getProfilingLogger().flush();
              } catch (DbException e) {
                LOGGER.error("Error flushing profiling logger", e);
              }
            }
            if (failFragments.isEmpty()) {
              executionFuture.setSuccess();
            } else {
              Throwable existingCause = executionFuture.getCause();
              Throwable newCause = failFragments.peek().getExecutionFuture().getCause();
              if (existingCause == null) {
                executionFuture.setFailure(newCause);
              } else {
                existingCause.addSuppressed(newCause);
              }
            }
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("New finished fragment: {}. {} remain.", drivingFragment,
                  (fragments.size() - currentNumFinished));
            }
          }
        }

      };

  /**
   * @param plan the plan of this {@link WorkerSubQuery}.
   * @param subQueryId the id of this subquery.
   * @param ownerWorker the worker on which this {@link WorkerSubQuery} is going to run
   */
  public WorkerSubQuery(final SubQueryPlan plan, final SubQueryId subQueryId,
      final Worker ownerWorker) {
    super(subQueryId, plan.getFTMode(), plan.getProfilingMode());
    List<RootOperator> operators = plan.getRootOps();
    fragments = new HashSet<LocalFragment>(operators.size());
    numFinishedFragments = new AtomicInteger(0);
    worker = ownerWorker;
    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
    for (final RootOperator fragmentRootOp : operators) {
      createFragment(fragmentRootOp);
    }
  }

  /**
   * create a {@link LocalFragment}.
   * 
   * @param root the root operator of the {@link LocalFragment}.
   * @return the {@link LocalFragment}.
   */
  public LocalFragment createFragment(final RootOperator root) {
    final LocalFragment drivingFragment =
        new LocalFragment(worker.getIPCConnectionPool(), this, root, worker.getQueryExecutor());
    LocalFragmentFuture fragmentExecutionFuture = drivingFragment.getExecutionFuture();
    fragmentExecutionFuture.addListener(fragmentExecutionListener);

    fragments.add(drivingFragment);

    return drivingFragment;
  }

  @Override
  public final LocalSubQueryFuture getExecutionFuture() {
    return executionFuture;
  }

  @Override
  public final void init() {
    for (LocalFragment t : fragments) {
      init(t);
    }
  }

  /**
   * initialize a {@link LocalFragment}.
   * 
   * @param f the {@link LocalFragment}
   */
  public final void init(final LocalFragment f) {
    ImmutableMap.Builder<String, Object> queryExecEnvVars = ImmutableMap.builder();
    queryExecEnvVars.put(MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getSubQueryId().getQueryId());
    f.init(queryExecEnvVars.putAll(worker.getExecEnvVars()).build());
  }

  @Override
  public final String toString() {
    return fragments.toString();
  }

  @Override
  public final void startExecution() {
    LOGGER.info("Subquery #{} start processing", getSubQueryId());
    getExecutionStatistics().markStart();
    if (getProfilingMode().contains(ProfilingMode.RESOURCE)) {
      resourceReportTimer = new Timer();
      resourceReportTimer.scheduleAtFixedRate(new ResourceUsageReporter(), 0,
          MyriaConstants.RESOURCE_REPORT_INTERVAL);
    }
    startMilliseconds = System.currentTimeMillis();
    for (LocalFragment t : fragments) {
      t.start();
    }
  }

  /** Send resource usage reports to server periodically. */
  private class ResourceUsageReporter extends ErrorLoggingTimerTask {
    @Override
    public synchronized void runInner() {
      for (LocalFragment fragment : fragments) {
        fragment.collectResourceMeasurements();
      }
    }
  }

  @Override
  public final void kill() {
    for (LocalFragment fragment : fragments) {
      fragment.kill();
    }
  }

  @Override
  public Set<Integer> getMissingWorkers() {
    return missingWorkers;
  }

  /**
   * add a recovery task for the failed worker.
   * 
   * @param workerId the id of the failed worker.
   */
  public void addRecoveryTasks(final int workerId) {
    int newOpId = getMaxOpId() + 1;
    List<RootOperator> recoveryTasks = new ArrayList<>();
    for (LocalFragment fragment : fragments) {
      if (fragment.getRootOp() instanceof Producer) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("adding recovery task for {}", fragment.getRootOp().getOpName());
        }
        List<StreamingState> buffers = ((Producer) fragment.getRootOp()).getTriedToSendTuples();
        List<Integer> indices =
            ((Producer) fragment.getRootOp()).getChannelIndicesOfAWorker(workerId);
        StreamOutputChannel<TupleBatch>[] channels =
            ((Producer) fragment.getRootOp()).getChannels();
        for (int i = 0; i < indices.size(); ++i) {
          int j = indices.get(i);
          /*
           * buffers.get(j) might be an empty List<TupleBatch>, so need to set its schema
           * explicitly.
           */
          TupleSource scan =
              new TupleSource(buffers.get(j).exportState(), buffers.get(j).getSchema());
          scan.setOpId(newOpId);
          newOpId++;
          scan.setOpName("tuplesource for " + fragment.getRootOp().getOpName()
              + channels[j].getID());
          RecoverProducer rp =
              new RecoverProducer(scan, ExchangePairID.fromExisting(channels[j].getID()
                  .getStreamID()), channels[j].getID().getRemoteID(),
                  (Producer) fragment.getRootOp(), j);
          rp.setOpId(newOpId);
          newOpId++;
          rp.setOpName("recProducer_for_" + fragment.getRootOp().getOpName());
          recoveryTasks.add(rp);
          scan.setFragmentId(0 - recoveryTasks.size());
          rp.setFragmentId(0 - recoveryTasks.size());
        }
      }
    }
    final List<LocalFragment> list = new ArrayList<>();
    for (RootOperator cp : recoveryTasks) {
      LocalFragment recoveryTask = createFragment(cp);
      list.add(recoveryTask);
    }
    new Thread() {
      @Override
      public void run() {
        while (true) {
          if (worker.getIPCConnectionPool().isRemoteAlive(workerId)) {
            /* waiting for ADD_WORKER to be received */
            for (LocalFragment fragment : list) {
              init(fragment);
              /* input might be null but we still need it to run */
              fragment.start();
              fragment.notifyNewInput();
            }
            break;
          }
          try {
            Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_100_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }.start();
  }

  /**
   * @return the owner worker
   */
  public Worker getWorker() {
    return worker;
  }

  /**
   * @return the time in milliseconds when the {@link WorkerSubQuery} was initialized.
   */
  public long getBeginMilliseconds() {
    return startMilliseconds;
  }

  @Override
  public Set<LocalFragment> getFragments() {
    return fragments;
  }

  /**
   * 
   * @return the max op id in this fragment.
   */
  private int getMaxOpId() {
    int ret = Integer.MIN_VALUE;
    for (LocalFragment t : fragments) {
      ret = Math.max(ret, t.getMaxOpId(t.getRootOp()));
    }
    return ret;
  }
}
