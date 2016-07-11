package edu.washington.escience.myria.parallel;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.LeapFrogJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.util.AtomicUtils;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.JVMUtils;
import edu.washington.escience.myria.util.concurrent.ReentrantSpinLock;

/**
 * Non-blocking driving code for one of the fragments in a {@link LocalSubQuery}.
 *
 * {@link LocalFragment} state could be:<br>
 * 1) In execution.<br>
 * 2) In dormant.<br>
 * 3) Already finished.<br>
 * 4) Has not started.
 */
public final class LocalFragment {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(LocalFragment.class);

  /**
   * The root operator.
   */
  private final RootOperator root;

  /**
   * The executor who is responsible for executing this fragment.
   */
  private final ExecutorService myExecutor;

  /**
   * Each bit for each output channel. Currently, if a single output channel is not writable, the whole
   * {@link LocalFragment} stops.
   */
  private final BitSet outputChannelAvailable;

  /**
   * the subquery of which this {@link LocalFragment} is a part.
   */
  private final LocalSubQuery localSubQuery;

  /**
   * The actual physical plan to be executed when this {@link LocalFragment} is run.
   */
  private final Callable<Void> executionPlan;

  /**
   * Task for executing initialization code .
   */
  private final Callable<Void> initTask;

  /**
   * The output channels belonging to this {@link LocalFragment}.
   */
  private final StreamIOChannelID[] outputChannels;

  /**
   * The IDBController operators, if any, in this {@link LocalFragment}.
   */
  private final Set<IDBController> idbControllerSet;

  /**
   * IPC ID of the owner {@link Worker} or {@link Server}.
   */
  private final int ipcEntityID;

  /**
   * The handle for managing the execution of the {@link LocalFragment}.
   */
  private volatile Future<Void> executionHandle;

  /**
   * Protect the output channel status.
   */
  private final ReentrantSpinLock outputLock = new ReentrantSpinLock();

  /**
   * Future for the {@link LocalFragment} execution.
   */
  private final LocalFragmentFuture fragmentExecutionFuture;

  /**
   * The lock mainly to make operator memory consistency.
   */
  private final Object executionLock = new Object();

  /**
   * resource manager.
   */
  private final LocalFragmentResourceManager resourceManager;

  /**
   * Record nanoseconds so that we can normalize the time in {@link ProfilingLogger}.
   */
  private volatile long beginNanoseconds;

  /**
   * Record the milliseconds so that we can calculate the difference to the worker initialization in
   * {@link ProfilingLogger}.
   */
  private volatile long beginMilliseconds = 0;

  /**
   * @return the fragment execution future.
   */
  LocalFragmentFuture getExecutionFuture() {
    return fragmentExecutionFuture;
  }

  /** total used CPU time of this task so far. */
  private volatile long cpuTotal = 0;
  /** total used CPU time of this task before starting the current execution. */
  private volatile long cpuBefore = 0;
  /** the thread id of this task. */
  private volatile long threadId = -1;

  /**
   * @param connectionPool the IPC connection pool.
   * @param localSubQuery the {@link LocalSubQuery} of which this {@link LocalFragment} is a part.
   * @param root the root operator this fragment will run.
   * @param executor the executor who provides the execution service for the fragment to run on
   */
  LocalFragment(
      final IPCConnectionPool connectionPool,
      final LocalSubQuery localSubQuery,
      final RootOperator root,
      final ExecutorService executor) {
    ipcEntityID = connectionPool.getMyIPCID();
    resourceManager = new LocalFragmentResourceManager(connectionPool, this);

    executionCondition = new AtomicInteger(STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE);
    this.root = root;
    myExecutor = executor;
    this.localSubQuery = localSubQuery;
    fragmentExecutionFuture = new LocalFragmentFuture(this, true);
    idbControllerSet = new HashSet<IDBController>();
    HashSet<StreamIOChannelID> outputChannelSet = new HashSet<StreamIOChannelID>();
    collectDownChannels(root, outputChannelSet);
    outputChannels = outputChannelSet.toArray(new StreamIOChannelID[] {});
    HashMap<StreamIOChannelID, Consumer> inputChannelMap =
        new HashMap<StreamIOChannelID, Consumer>();
    collectUpChannels(root, inputChannelMap);
    for (final Consumer operator : inputChannelMap.values()) {
      resourceManager.allocateInputBuffer(operator);
    }
    Arrays.sort(outputChannels);
    outputChannelAvailable = new BitSet(outputChannels.length);
    for (int i = 0; i < outputChannels.length; i++) {
      outputChannelAvailable.set(i);
    }

    executionPlan =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // synchronized to keep memory consistency
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Start fragment execution: " + LocalFragment.this);
            }
            if (threadId == -1) {
              threadId = Thread.currentThread().getId();
            }
            // otherwise threadId should always equal to Thread.currentThread().getId() based on the current design

            Set<ProfilingMode> mode = localSubQuery.getProfilingMode();
            if (mode.contains(ProfilingMode.RESOURCE)) {
              synchronized (LocalFragment.this) {
                cpuBefore = ManagementFactory.getThreadMXBean().getThreadCpuTime(threadId);
              }
            }
            try {
              synchronized (executionLock) {
                LocalFragment.this.executeActually();
              }
            } catch (RuntimeException e) {
              LOGGER.error("Unexpected RuntimeException: ", e);
              throw e;
            } finally {
              executionHandle = null;
            }
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("End execution: " + LocalFragment.this);
            }

            if (mode.contains(ProfilingMode.RESOURCE)) {
              synchronized (LocalFragment.this) {
                cpuTotal +=
                    ManagementFactory.getThreadMXBean().getThreadCpuTime(threadId) - cpuBefore;
                cpuBefore = 0;
              }
            }
            return null;
          }
        };

    initTask =
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            // synchronized to keep memory consistency
            LOGGER.trace("Start fragment initialization: ", LocalFragment.this);
            try {
              synchronized (executionLock) {
                LocalFragment.this.initActually();
              }
            } catch (Throwable e) {
              LOGGER.error("Fragment failed to open because of exception:", e);
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              } else if (e instanceof OutOfMemoryError) {
                JVMUtils.shutdownVM(e);
              }
              AtomicUtils.setBitByValue(executionCondition, STATE_FAIL);
              if (fragmentExecutionFuture.setFailure(e)) {
                cleanup(true);
              }
            }
            LOGGER.trace("End fragment initialization: {}", LocalFragment.this);
            return null;
          }
        };
  }

  /**
   * @return all output channels belonging to this {@link LocalFragment}.
   */
  StreamIOChannelID[] getOutputChannels() {
    return outputChannels;
  }

  /**
   * @return all the IDBController operators in this {@link LocalFragment}.
   */
  Set<IDBController> getIDBControllers() {
    return idbControllerSet;
  }

  /**
   * gather all output (Producer or IDBController's EOI report) channel IDs.
   *
   * @param currentOperator current operator to check.
   * @param outputExchangeChannels the current collected output channel IDs.
   */
  private void collectDownChannels(
      final Operator currentOperator, final HashSet<StreamIOChannelID> outputExchangeChannels) {

    if (currentOperator instanceof Producer) {
      Producer p = (Producer) currentOperator;
      StreamIOChannelID[] exCID = p.getOutputChannelIDs(ipcEntityID);
      for (StreamIOChannelID element : exCID) {
        outputExchangeChannels.add(element);
      }
    } else if (currentOperator instanceof IDBController) {
      IDBController p = (IDBController) currentOperator;
      ExchangePairID oID = p.getControllerOperatorID();
      int wID = p.getControllerWorkerID();
      outputExchangeChannels.add(new StreamIOChannelID(oID.getLong(), wID));
      idbControllerSet.add(p);
    }

    final Operator[] children = currentOperator.getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          collectDownChannels(child, outputExchangeChannels);
        }
      }
    }
  }

  /**
   * @return if the {@link LocalFragment} is finished
   */
  public boolean isFinished() {
    return fragmentExecutionFuture.isDone();
  }

  /**
   * gather all input (consumer) channel IDs.
   *
   * @param currentOperator current operator to check.
   * @param inputExchangeChannels the current collected input channels.
   */
  private void collectUpChannels(
      final Operator currentOperator,
      final Map<StreamIOChannelID, Consumer> inputExchangeChannels) {

    if (currentOperator instanceof Consumer) {
      Consumer c = (Consumer) currentOperator;
      int[] sourceWorkers = c.getSourceWorkers(ipcEntityID);
      ExchangePairID oID = c.getOperatorID();
      for (int sourceWorker : sourceWorkers) {
        inputExchangeChannels.put(new StreamIOChannelID(oID.getLong(), sourceWorker), c);
      }
    }

    final Operator[] children = currentOperator.getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          collectUpChannels(child, inputExchangeChannels);
        }
      }
    }
  }

  /**
   * call this method if a new TupleBatch arrived at a Consumer operator belonging to this {@link LocalFragment}. This
   * method is always called by Netty Upstream IO worker threads.
   */
  public void notifyNewInput() {
    AtomicUtils.setBitByValue(executionCondition, STATE_INPUT_AVAILABLE);
    execute();
  }

  /**
   * Called by Netty downstream IO worker threads.
   *
   * @param outputChannelID the logical output channel ID.
   */
  public void notifyOutputDisabled(final StreamIOChannelID outputChannelID) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Output disabled: " + outputChannelID);
    }
    int idx = Arrays.binarySearch(outputChannels, outputChannelID);
    outputLock.lock();
    try {
      outputChannelAvailable.clear(idx);
    } finally {
      outputLock.unlock();
    }
    // disable output
    AtomicUtils.unsetBitByValue(executionCondition, STATE_OUTPUT_AVAILABLE);
  }

  /**
   * Called by Netty downstream IO worker threads.
   *
   * @param outputChannelID the down channel ID.
   */
  public void notifyOutputEnabled(final StreamIOChannelID outputChannelID) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Output enabled: " + outputChannelID);
    }
    if (!isOutputAvailable()) {
      int idx = Arrays.binarySearch(outputChannels, outputChannelID);
      outputLock.lock();
      try {
        if (!outputChannelAvailable.get(idx)) {
          outputChannelAvailable.set(idx);
          if (outputChannelAvailable.nextClearBit(0) >= outputChannels.length) {
            // enable output
            AtomicUtils.setBitByValue(executionCondition, STATE_OUTPUT_AVAILABLE);
            execute();
          }
        }
      } finally {
        outputLock.unlock();
      }
    }
  }

  @Override
  public String toString() {
    SubQueryId queryID = localSubQuery.getSubQueryId();
    Operator rootOp = root;

    StringBuilder stateS = new StringBuilder();
    int state = executionCondition.get();
    String splitter = "";
    if ((state & LocalFragment.STATE_INITIALIZED) == STATE_INITIALIZED) {
      stateS.append(splitter + "Initialized");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_STARTED) == STATE_STARTED) {
      stateS.append(splitter + "Started");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_INPUT_AVAILABLE) == STATE_INPUT_AVAILABLE) {
      stateS.append(splitter + "Input_Available");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_EOS) == STATE_EOS) {
      stateS.append(splitter + "EOS");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_EXECUTION_REQUESTED) == STATE_EXECUTION_REQUESTED) {
      stateS.append(splitter + "Execution_Requested");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_IN_EXECUTION) == STATE_IN_EXECUTION) {
      stateS.append(splitter + "In_Execution");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_KILLED) == STATE_KILLED) {
      stateS.append(splitter + "Killed");
      splitter = " | ";
    }
    if ((state & LocalFragment.STATE_OUTPUT_AVAILABLE) == STATE_OUTPUT_AVAILABLE) {
      stateS.append(splitter + "Output_Available");
      splitter = " | ";
    }
    return String.format(
        "%s: { Owner QID: %s, Root Op: %s, State: %s }",
        LocalFragment.class.getSimpleName(),
        queryID,
        rootOp,
        stateS.toString());
  }

  /**
   * Actually execute this fragment.
   *
   * @return always null. The return value is unused.
   */
  private Object executeActually() {
    beginNanoseconds = System.nanoTime();
    beginMilliseconds = System.currentTimeMillis();

    Throwable failureCause = null;
    if (executionCondition.compareAndSet(
        EXECUTION_READY | STATE_EXECUTION_REQUESTED,
        EXECUTION_READY | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION)) {
      EXECUTE:
      while (true) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "LocalFragment execution interrupted. Root operator: "
                    + root
                    + ". Close directly.");
          }

          // set interrupted
          AtomicUtils.setBitByValue(executionCondition, STATE_INTERRUPTED);

          // TODO clean up fragment state
        } else {
          // do the execution

          // clearInput(); // clear input at beginning.
          AtomicUtils.unsetBitByValue(executionCondition, STATE_INPUT_AVAILABLE);

          boolean breakByOutputUnavailable = false;
          try {
            boolean hasData = true;
            while (hasData && !breakByOutputUnavailable) {
              hasData = false;
              if (root.nextReady() != null) {
                hasData = true;
                breakByOutputUnavailable = !isOutputAvailable();
              } else {
                // check output
                if (root.eoi()) {
                  root.setEOI(false);
                  hasData = true;
                  breakByOutputUnavailable = !isOutputAvailable();
                }
              }
              if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          } catch (final Throwable e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Unexpected exception occur at operator excution. Operator: " + root, e);
            }
            failureCause = e;
            AtomicUtils.setBitByValue(executionCondition, STATE_FAIL);
            if (e instanceof OutOfMemoryError) {
              JVMUtils.shutdownVM(e);
            }
          }

          if (breakByOutputUnavailable) {
            // we do not know whether all the inputs have been consumed, recover the input available bit
            AtomicUtils.setBitByValue(executionCondition, STATE_INPUT_AVAILABLE);
          }
        }

        // Check if another round of execution is needed.
        int oldV = executionCondition.get();
        while (oldV != EXECUTION_CONTINUE) {
          // try clear the STATE_EXECUTION_REQUESTED and STATE_IN_EXECUTION bit
          if (executionCondition.compareAndSet(
              oldV, oldV & ~(STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION))) {
            // exit execution.
            break EXECUTE;
          }
          oldV = executionCondition.get();
        }
      }

      // clear interrupted
      AtomicUtils.unsetBitByValue(executionCondition, STATE_INTERRUPTED);
      Thread.interrupted();

    } else {
      AtomicUtils.unsetBitByValue(executionCondition, STATE_EXECUTION_REQUESTED);
    }

    if ((executionCondition.get() & STATE_FAIL) == STATE_FAIL) {
      // failed
      if (fragmentExecutionFuture.setFailure(failureCause)) {
        cleanup(true);
      }
    } else if (root.eos()) {
      if (AtomicUtils.setBitIfUnsetByValue(executionCondition, STATE_EOS)) {
        cleanup(false);
        fragmentExecutionFuture.setSuccess();
      }
    } else if ((executionCondition.get() & STATE_KILLED) == STATE_KILLED) {
      // killed
      if (fragmentExecutionFuture.setFailure(
          new QueryKilledException("LocalFragment was killed"))) {
        cleanup(true);
      }
    }
    return null;
  }

  /**
   * Current execution condition.
   */
  private final AtomicInteger executionCondition;

  /**
   * The {@link LocalFragment} is initialized.
   */
  private static final int STATE_INITIALIZED = (1 << 0);

  /**
   * The {@link LocalFragment} has actually been started.
   */
  private static final int STATE_STARTED = (1 << 1);

  /**
   * All outputs of the {@link LocalFragment} are available.
   */
  private static final int STATE_OUTPUT_AVAILABLE = (1 << 2);

  /**
   * @return if the output channels are available for writing.
   */
  private boolean isOutputAvailable() {
    return (executionCondition.get() & STATE_OUTPUT_AVAILABLE) == STATE_OUTPUT_AVAILABLE;
  }

  /**
   * Any input of the {@link LocalFragment} is available.
   */
  private static final int STATE_INPUT_AVAILABLE = (1 << 3);

  /**
   * The {@link LocalFragment} is killed.
   */
  private static final int STATE_KILLED = (1 << 4);

  /**
   * The {@link LocalFragment} is EOS.
   */
  private static final int STATE_EOS = (1 << 5);

  /**
   * The {@link LocalFragment} has been requested to execute.
   */
  private static final int STATE_EXECUTION_REQUESTED = (1 << 6);

  /**
   * The {@link LocalFragment} fails because of uncaught exception.
   */
  private static final int STATE_FAIL = (1 << 7);

  /**
   * The {@link LocalFragment} execution thread is interrupted.
   */
  private static final int STATE_INTERRUPTED = (1 << 8);

  /**
   * The {@link LocalFragment} is in execution.
   */
  private static final int STATE_IN_EXECUTION = (1 << 9);

  /**
   * Non-blocking ready condition.
   */
  public static final int EXECUTION_PRE_START =
      STATE_INITIALIZED | STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE;

  /**
   * Non-blocking ready condition.
   */
  public static final int EXECUTION_READY = EXECUTION_PRE_START | STATE_STARTED;

  /**
   * Non-blocking continue execution condition.
   */
  public static final int EXECUTION_CONTINUE =
      EXECUTION_READY | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION;

  /**
   * @return if the {@link LocalFragment} has been killed.
   */
  public boolean isKilled() {
    return (executionCondition.get() & STATE_KILLED) == STATE_KILLED;
  }

  /**
   * clean up the {@link LocalFragment}, release resources, etc.
   *
   * @param failed if the {@link LocalFragment} execution has already failed.
   */
  private void cleanup(final boolean failed) {
    if (getLocalSubQuery().getProfilingMode().contains(ProfilingMode.RESOURCE)) {
      // Before everything is cleaned up, get the latest resource stats.
      collectResourceMeasurements();
    }
    if (AtomicUtils.unsetBitIfSetByValue(executionCondition, STATE_INITIALIZED)) {
      // Only cleanup if initialized.
      try {
        root.close();
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception at operator close. Root operator: " + root + ".", ee);
        }
        if (!failed) {
          fragmentExecutionFuture.setFailure(ee);
        }
      } finally {
        if (resourceManager != null) {
          resourceManager.cleanup();
        }
      }
    }
  }

  /**
   * Kill this {@link LocalFragment}.
   *
   */
  void kill() {
    if (!AtomicUtils.setBitIfUnsetByValue(executionCondition, STATE_KILLED)) {
      return;
    }

    final Future<Void> executionHandleLocal = executionHandle;
    if (executionHandleLocal != null) {
      // Abruptly cancel the execution
      executionHandleLocal.cancel(true);
    }

    myExecutor.submit(executionPlan);
  }

  /**
   * Start this {@link LocalFragment}.
   */
  public void start() {
    AtomicUtils.setBitByValue(executionCondition, STATE_STARTED);
    execute();
  }

  /**
   * Execute this {@link LocalFragment}.
   */
  private void execute() {

    if (executionCondition.compareAndSet(
        EXECUTION_READY, EXECUTION_READY | STATE_EXECUTION_REQUESTED)) {
      // set in execution.
      executionHandle = myExecutor.submit(executionPlan);
    }
  }

  /**
   * Execution environment variable.
   */
  private volatile ImmutableMap<String, Object> execEnvVars;

  /**
   * Initialize the {@link LocalFragment}.
   *
   * @param execEnvVars execution environment variable.
   */
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    this.execEnvVars = execEnvVars;
    try {
      myExecutor.submit(initTask).get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      return;
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  /**
   * The actual initialization method.
   *
   * @throws Exception if any exception occurs.
   */
  private void initActually() throws Exception {
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    b.put(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER, resourceManager);
    b.putAll(execEnvVars);
    root.open(b.build());
    AtomicUtils.setBitByValue(executionCondition, STATE_INITIALIZED);
  }

  /**
   * Return the {@link LocalSubQuery} of which this {@link LocalFragment} is a part.
   *
   * @return the {@link LocalSubQuery} of which this {@link LocalFragment} is a part
   */
  public LocalSubQuery getLocalSubQuery() {
    return localSubQuery;
  }

  /**
   * enable/disable output channels of the root(producer) of this {@link LocalFragment}.
   *
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    if (root instanceof Producer) {
      ((Producer) root).updateChannelAvailability(workerId, enable);
    }
  }

  /**
   * return the root operator of this {@link LocalFragment}.
   *
   * @return the root operator
   */
  public RootOperator getRootOp() {
    return root;
  }

  /**
   * return the resource manager of this {@link LocalFragment}.
   *
   * @return the resource manager.
   */
  public LocalFragmentResourceManager getResourceManager() {
    return resourceManager;
  }

  /**
   * @return the nanosecond counter when this {@link LocalFragment} began executing.
   */
  public long getBeginNanoseconds() {
    return beginNanoseconds;
  }

  /**
   * @return the time when this {@link LocalFragment} began executing.
   */
  public long getBeginMilliseconds() {
    return beginMilliseconds;
  }

  /**
   *
   * @param stats the stats to be added into
   * @param timestamp the timestamp of the collecting event
   * @param op the operator
   * @param measurement which measurement
   * @param value the value
   * @param subQueryId the sunquery ID
   */
  public void addResourceReport(
      final List<ResourceStats> stats,
      final long timestamp,
      final Operator op,
      final String measurement,
      final long value,
      final SubQueryId subQueryId) {
    int opId = Preconditions.checkNotNull(op.getOpId(), "opId is null");
    stats.add(
        new ResourceStats(
            timestamp,
            opId,
            measurement,
            value,
            subQueryId.getQueryId(),
            subQueryId.getSubqueryId()));
  }

  /**
   *
   * @param stats the stats
   * @param op the current operator
   * @param timestamp the starting timestamp of this event in milliseconds
   * @param subQueryId the subQuery Id
   */
  public void collectOperatorResourceMeasurements(
      final List<ResourceStats> stats,
      final long timestamp,
      final Operator op,
      final SubQueryId subQueryId) {
    if (op instanceof Producer) {
      addResourceReport(
          stats,
          timestamp,
          op,
          "numTuplesWritten",
          ((Producer) op).getNumTuplesWrittenToChannels(),
          subQueryId);
      addResourceReport(
          stats,
          timestamp,
          op,
          "numTuplesInBuffers",
          ((Producer) op).getNumTuplesInBuffers(),
          subQueryId);
    } else if (op instanceof IDBController) {
      addResourceReport(
          stats,
          timestamp,
          op,
          "numTuplesInState",
          ((IDBController) op).getStreamingState().numTuples(),
          subQueryId);
    } else if (op instanceof SymmetricHashJoin) {
      addResourceReport(
          stats,
          timestamp,
          op,
          "hashTableSize",
          ((SymmetricHashJoin) op).getNumTuplesInHashTables(),
          subQueryId);
    } else if (op instanceof LeapFrogJoin) {
      addResourceReport(
          stats,
          timestamp,
          op,
          "hashTableSize",
          ((LeapFrogJoin) op).getNumTuplesInHashTables(),
          subQueryId);
    }
    for (Operator child : op.getChildren()) {
      collectOperatorResourceMeasurements(stats, timestamp, child, subQueryId);
    }
  }

  /**
   *
   */
  public void collectResourceMeasurements() {
    if (!(getLocalSubQuery() instanceof WorkerSubQuery)) {
      /* MasterSubQuery does not support profiling so far, revisit if it changes */
      return;
    }
    if (threadId == -1) {
      /* has not been executed */
      return;
    }
    WorkerSubQuery subQuery = (WorkerSubQuery) getLocalSubQuery();

    RootOperator root = getRootOp();
    long cntCpu = 0;
    synchronized (this) {
      cntCpu = cpuTotal;
      if (cpuBefore > 0) {
        cntCpu += ManagementFactory.getThreadMXBean().getThreadCpuTime(threadId) - cpuBefore;
      }
    }
    List<ResourceStats> resourceUsage = new ArrayList<ResourceStats>();
    long timestamp = System.currentTimeMillis();
    SubQueryId subQueryId = subQuery.getSubQueryId();
    addResourceReport(resourceUsage, timestamp, root, "cpuTotal", cntCpu, subQueryId);

    collectOperatorResourceMeasurements(resourceUsage, timestamp, root, subQueryId);

    Worker worker = subQuery.getWorker();
    worker.sendMessageToMaster(IPCUtils.resourceReport(resourceUsage)).awaitUninterruptibly();
    try {
      for (ResourceStats stats : resourceUsage) {
        worker.getProfilingLogger().recordResource(stats);
      }
    } catch (DbException e) {
      LOGGER.error("Error inserting resource stats into profiling logger", e);
    }
  }

  /**
   * @param op the current operator.
   * @return the max op id in this subtree.
   */
  public int getMaxOpId(final Operator op) {
    int ret = Preconditions.checkNotNull(op.getOpId(), "opId");
    for (Operator child : op.getChildren()) {
      ret = Math.max(ret, getMaxOpId(child));
    }
    return ret;
  }
}
