package edu.washington.escience.myria.parallel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingState;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.operator.network.RecoverProducer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * A {@link LocalSubQuery} running at a worker.
 */
public class WorkerSubQuery implements LocalSubQuery {

  /**
   * logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerSubQuery.class);

  /**
   * The ID of this subquery.
   */
  private final SubQueryId subQueryId;

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
   * The ftMode.
   */
  private final FTMODE ftMode;

  /**
   * The profiling mode.
   */
  private final boolean profilingMode;

  /**
   * priority, currently no use.
   */
  private volatile int priority;

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
   */
  private volatile long startMilliseconds = 0;

  /**
   * The future listener for processing the complete events of the execution of all the subquery's fragments.
   */
  private final LocalFragmentFutureListener fragmentExecutionListener = new LocalFragmentFutureListener() {

    @Override
    public void operationComplete(final LocalFragmentFuture future) throws Exception {
      LocalFragment drivingFragment = future.getFragment();
      int currentNumFinished = numFinishedFragments.incrementAndGet();

      executionFuture.setProgress(1, currentNumFinished, fragments.size());
      Throwable failureReason = future.getCause();
      if (!future.isSuccess()) {
        failFragments.add(drivingFragment);
        if (!(failureReason instanceof QueryKilledException)) {
          // The fragment is a failure, not killed.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("got a failed fragment, root op = {} cause {}", drivingFragment.getRootOp().getOpName(),
                failureReason);
          }
          for (LocalFragment t : fragments) {
            // kill all the other {@link Fragment}s.
            t.kill();
          }
        }
      }

      if (currentNumFinished >= fragments.size()) {
        queryStatistics.markEnd();
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Query #{} executed for {}", subQueryId, DateTimeUtils.nanoElapseToHumanReadable(queryStatistics
              .getQueryExecutionElapse()));
        }
        if (isProfilingMode()) {
          try {
            getWorker().getProfilingLogger().flush();
          } catch (DbException e) {
            LOGGER.error("error flushing Profiling Logger: ", e);
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
   * Statistics of this {@link WorkerSubQuery}.
   */
  private final ExecutionStatistics queryStatistics = new ExecutionStatistics();

  /**
   * @param plan the plan of this {@link WorkerSubQuery}.
   * @param subQueryId the id of this subquery.
   * @param ownerWorker the worker on which this {@link WorkerSubQuery} is going to run
   */
  public WorkerSubQuery(final SubQueryPlan plan, final SubQueryId subQueryId, final Worker ownerWorker) {
    this.subQueryId = subQueryId;
    ftMode = plan.getFTMode();
    profilingMode = plan.isProfilingMode();
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
        new LocalFragment(worker.getIPCConnectionPool().getMyIPCID(), this, root, worker.getQueryExecutor());
    LocalFragmentFuture fragmentExecutionFuture = drivingFragment.getExecutionFuture();
    fragmentExecutionFuture.addListener(fragmentExecutionListener);

    fragments.add(drivingFragment);

    HashSet<Consumer> consumerSet = new HashSet<>();
    consumerSet.addAll(drivingFragment.getInputChannels().values());
    for (final Consumer c : consumerSet) {
      FlowControlBagInputBuffer<TupleBatch> inputBuffer =
          new FlowControlBagInputBuffer<TupleBatch>(worker.getIPCConnectionPool(), c.getInputChannelIDs(worker
              .getIPCConnectionPool().getMyIPCID()), worker.getInputBufferCapacity(), worker
              .getInputBufferRecoverTrigger(), worker.getIPCConnectionPool());
      inputBuffer.addListener(FlowControlBagInputBuffer.NEW_INPUT_DATA, new IPCEventListener() {
        @Override
        public void triggered(final IPCEvent event) {
          drivingFragment.notifyNewInput();
        }
      });
      c.setInputBuffer(inputBuffer);
    }

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
    LocalFragmentResourceManager resourceManager = new LocalFragmentResourceManager(worker.getIPCConnectionPool(), f);
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    f.init(resourceManager, b.putAll(worker.getExecEnvVars()).build());
  }

  @Override
  public final SubQueryId getSubQueryId() {
    return subQueryId;
  }

  @Override
  public final int compareTo(final LocalSubQuery o) {
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
    return fragments + ", priority:" + priority;
  }

  @Override
  public final void startExecution() {
    LOGGER.info("Query : {} start processing", subQueryId);

    startMilliseconds = System.currentTimeMillis();

    queryStatistics.markStart();
    for (LocalFragment t : fragments) {
      t.start();
    }
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  @Override
  public final void kill() {
    for (LocalFragment fragment : fragments) {
      fragment.kill();
    }
  }

  @Override
  public final ExecutionStatistics getExecutionStatistics() {
    return queryStatistics;
  }

  @Override
  public FTMODE getFTMode() {
    return ftMode;
  }

  @Override
  public boolean isProfilingMode() {
    return profilingMode;
  }

  @Override
  public Set<Integer> getMissingWorkers() {
    return missingWorkers;
  }

  /**
   * when a REMOVE_WORKER message is received, give all the {@link LocalFragment}s of this {@link SubQuery} another
   * chance to decide if they are ready to generate EOS/EOI.
   */
  public void triggerFragmentEosEoiChecks() {
    for (LocalFragment fragment : fragments) {
      fragment.notifyNewInput();
    }
  }

  /**
   * enable/disable output channels of the root(producer) of each fragment.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    for (LocalFragment fragment : fragments) {
      fragment.updateProducerChannels(workerId, enable);
    }
  }

  /**
   * add a recovery task for the failed worker.
   * 
   * @param workerId the id of the failed worker.
   */
  public void addRecoveryTasks(final int workerId) {
    List<RootOperator> recoveryTasks = new ArrayList<>();
    for (LocalFragment fragment : fragments) {
      if (fragment.getRootOp() instanceof Producer) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("adding recovery task for {}", fragment.getRootOp().getOpName());
        }
        List<StreamingState> buffers = ((Producer) fragment.getRootOp()).getTriedToSendTuples();
        List<Integer> indices = ((Producer) fragment.getRootOp()).getChannelIndicesOfAWorker(workerId);
        StreamOutputChannel<TupleBatch>[] channels = ((Producer) fragment.getRootOp()).getChannels();
        for (int i = 0; i < indices.size(); ++i) {
          int j = indices.get(i);
          /* buffers.get(j) might be an empty List<TupleBatch>, so need to set its schema explicitly. */
          TupleSource scan = new TupleSource(buffers.get(j).exportState(), buffers.get(j).getSchema());
          scan.setOpName("tuplesource for " + fragment.getRootOp().getOpName() + channels[j].getID());
          RecoverProducer rp =
              new RecoverProducer(scan, ExchangePairID.fromExisting(channels[j].getID().getStreamID()), channels[j]
                  .getID().getRemoteID(), (Producer) fragment.getRootOp(), j);
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
}
