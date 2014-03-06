package edu.washington.escience.myria.parallel;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.util.AtomicUtils;
import edu.washington.escience.myria.util.concurrent.ExecutableExecutionFuture;
import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;
import edu.washington.escience.myria.util.concurrent.ReentrantSpinLock;

/**
 * Non-blocking driving code of a sub-query.
 * 
 * Task state could be:<br>
 * 1) In execution.<br>
 * 2) In dormant.<br>
 * 3) Already finished.<br>
 * 4) Has not started.
 * */
public final class QuerySubTreeTask {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QuerySubTreeTask.class.getName());

  /**
   * The logger for profiler.
   */
  private static final org.slf4j.Logger PROFILING_LOGGER = org.slf4j.LoggerFactory.getLogger("profile");

  /**
   * The root operator.
   * */
  private final RootOperator root;

  /**
   * Each bit for each output channel. Currently, if a single output channel is not writable, the whole task stops.
   * */
  private final BitSet outputChannelAvailable;

  /**
   * the owner query partition.
   * */
  private final QueryPartition ownerQuery;

  /**
   * Execution task.
   * */
  private final Callable<Void> executionTask;

  /**
   * The output channels belonging to this task.
   * */
  private final StreamIOChannelID[] outputChannels;

  /**
   * The output channels belonging to this task.
   * */
  private final Map<StreamIOChannelID, Consumer> inputChannels;

  /**
   * The IDBController operators in this task.
   * */
  private final Set<IDBController> idbControllerSet;

  /**
   * IPC ID of the owner {@link Worker} or {@link Server}.
   * */
  private final int ipcEntityID;

  /**
   * The handle for managing the execution of the task.
   * */
  private volatile Future<Void> executionHandle;

  /**
   * Protect the output channel status.
   */
  private final ReentrantSpinLock outputLock = new ReentrantSpinLock();

  /**
   * Future for the task execution.
   * */
  private final DefaultTaskFuture taskExecutionFuture;

  /**
   * The lock mainly to make operator memory consistency.
   * */
  private final Object executionLock = new Object();

  /**
   * resource manager.
   * */
  private volatile TaskResourceManager resourceManager;

  /**
   * @return the task execution future.
   */
  TaskFuture getExecutionFuture() {
    return taskExecutionFuture;
  }

  /**
   * @param ipcEntityID the IPC ID of the owner worker/master.
   * @param ownerQuery the owner query of this task.
   * @param root the root operator this task will run.
   */
  QuerySubTreeTask(final int ipcEntityID, final QueryPartition ownerQuery, final RootOperator root) {
    this.ipcEntityID = ipcEntityID;
    executionCondition = new AtomicInteger(STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE);
    this.root = root;
    this.ownerQuery = ownerQuery;
    taskExecutionFuture = new DefaultTaskFuture(this, true);
    idbControllerSet = new HashSet<IDBController>();
    HashSet<StreamIOChannelID> outputChannelSet = new HashSet<StreamIOChannelID>();
    collectDownChannels(root, outputChannelSet);
    outputChannels = outputChannelSet.toArray(new StreamIOChannelID[] {});
    HashMap<StreamIOChannelID, Consumer> inputChannelMap = new HashMap<StreamIOChannelID, Consumer>();
    collectUpChannels(root, inputChannelMap);
    inputChannels = inputChannelMap;
    Arrays.sort(outputChannels);
    outputChannelAvailable = new BitSet(outputChannels.length);
    for (int i = 0; i < outputChannels.length; i++) {
      outputChannelAvailable.set(i);
    }

    executionTask = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // synchronized to keep memory consistency
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Start task execution: " + QuerySubTreeTask.this);
        }
        try {
          synchronized (executionLock) {
            QuerySubTreeTask.this.executeActually();
          }
        } catch (RuntimeException ee) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Unexpected Error: ", ee);
          }
          throw ee;
        } finally {
          executionHandle = null;
        }
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("End execution: " + QuerySubTreeTask.this);
        }
        return null;
      }
    };
  }

  /**
   * @return all input channels belonging to this task.
   * */
  Map<StreamIOChannelID, Consumer> getInputChannels() {
    return inputChannels;
  }

  /**
   * @return all output channels belonging to this task.
   */
  StreamIOChannelID[] getOutputChannels() {
    return outputChannels;
  }

  /**
   * @return all the IDBController operators in this task.
   * */
  Set<IDBController> getIDBControllers() {
    return idbControllerSet;
  }

  /**
   * gather all output (Producer or IDBController's EOI report) channel IDs.
   * 
   * @param currentOperator current operator to check.
   * @param outputExchangeChannels the current collected output channel IDs.
   * */
  private void collectDownChannels(final Operator currentOperator,
      final HashSet<StreamIOChannelID> outputExchangeChannels) {

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
   * @return if the task is finished
   * */
  public boolean isFinished() {
    return taskExecutionFuture.isDone();
  }

  /**
   * gather all input (consumer) channel IDs.
   * 
   * @param currentOperator current operator to check.
   * @param inputExchangeChannels the current collected input channels.
   * */
  private void collectUpChannels(final Operator currentOperator,
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
   * call this method if a new TupleBatch arrived at a Consumer operator belonging to this task. This method is always
   * called by Netty Upstream IO worker threads.
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
   * */
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
    long queryID = ownerQuery.getQueryID();
    Operator rootOp = root;

    StringBuilder stateS = new StringBuilder();
    int state = executionCondition.get();
    String splitter = "";
    if ((state & QuerySubTreeTask.STATE_INITIALIZED) == STATE_INITIALIZED) {
      stateS.append(splitter + "Initialized");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_INPUT_AVAILABLE) == STATE_INPUT_AVAILABLE) {
      stateS.append(splitter + "Input_Available");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_EOS) == STATE_EOS) {
      stateS.append(splitter + "EOS");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_EXECUTION_REQUESTED) == STATE_EXECUTION_REQUESTED) {
      stateS.append(splitter + "Execution_Requested");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_IN_EXECUTION) == STATE_IN_EXECUTION) {
      stateS.append(splitter + "In_Execution");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_KILLED) == STATE_KILLED) {
      stateS.append(splitter + "Killed");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_PAUSED) == STATE_PAUSED) {
      stateS.append(splitter + "Paused");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_OUTPUT_AVAILABLE) == STATE_OUTPUT_AVAILABLE) {
      stateS.append(splitter + "Output_Available");
      splitter = " | ";
    }
    return String.format("Task: { Owner QID: %d, Root Op: %s, State: %s }", queryID, rootOp, stateS.toString());
  }

  /**
   * Actually execute this task.
   * 
   * @return always null. The return value is unused.
   * */
  private Object executeActually() {

    if (getOwnerQuery().isProfilingMode()) {
      PROFILING_LOGGER.info("[{}#{}][{}@{}][{}][{}]:set time", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, ownerQuery
          .getQueryID(), "startTimeInMS", root.getFragmentId(), System.currentTimeMillis(), 0);
      PROFILING_LOGGER.info("[{}#{}][{}@{}][{}][{}]:set time", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, ownerQuery
          .getQueryID(), "startTimeInNS", root.getFragmentId(), System.nanoTime(), 0);
    }

    Throwable failureCause = null;
    if (executionCondition.compareAndSet(EXECUTION_READY | STATE_EXECUTION_REQUESTED, EXECUTION_READY
        | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION)) {
      EXECUTE : while (true) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
          }

          // set interrupted
          AtomicUtils.setBitByValue(executionCondition, STATE_INTERRUPTED);

          // TODO clean up task state
        } else if (ownerQuery.isPaused()) {
          // the owner query is paused, exit execution.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution paused because the query is paused. Root operator: {}.", root);
          }
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
          if (executionCondition.compareAndSet(oldV, oldV & ~(STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION))) {
            // exit execution.
            break EXECUTE;
          }
          oldV = executionCondition.get();
        }

      }

      // clear interrupted
      AtomicUtils.unsetBitByValue(executionCondition, STATE_INTERRUPTED);
      Thread.interrupted();

    }

    if ((executionCondition.get() & STATE_FAIL) == STATE_FAIL) {
      // failed
      taskExecutionFuture.setFailure(failureCause);
      cleanup();
    } else if (root.eos()) {
      AtomicUtils.setBitIfUnsetByValue(executionCondition, STATE_EOS);
      cleanup();
      taskExecutionFuture.setSuccess();
    } else if ((executionCondition.get() & STATE_KILLED) == STATE_KILLED) {
      // killed
      taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
      cleanup();
    }
    return null;
  }

  /**
   * Current execution condition.
   * */
  private final AtomicInteger executionCondition;

  /**
   * The task is initialized.
   */
  private static final int STATE_INITIALIZED = 0x01;

  /**
   * All output of the task are available.
   */
  private static final int STATE_OUTPUT_AVAILABLE = 0x02;

  /**
   * @return if the output channels are available for writing.
   * */
  private boolean isOutputAvailable() {
    return (executionCondition.get() & STATE_OUTPUT_AVAILABLE) == STATE_OUTPUT_AVAILABLE;
  }

  /**
   * Any input of the task is available.
   */
  private static final int STATE_INPUT_AVAILABLE = 0x08;

  /**
   * The task is paused.
   */
  private static final int STATE_PAUSED = 0x10;

  /**
   * The task is killed.
   */
  private static final int STATE_KILLED = 0x20;

  /**
   * @return if the task is already get killed.
   * */
  public boolean isKilled() {
    return (executionCondition.get() & STATE_KILLED) == STATE_KILLED;
  }

  /**
   * The task is not EOS.
   * */
  private static final int STATE_EOS = 0x40;

  /**
   * The task is currently not in execution.
   * */
  private static final int STATE_EXECUTION_REQUESTED = 0x80;

  /**
   * The task fails because of uncaught exception.
   * */
  private static final int STATE_FAIL = 0x100;

  /**
   * The task execution thread is interrupted.
   * */
  private static final int STATE_INTERRUPTED = 0x200;

  /**
   * The task is in execution.
   * */
  private static final int STATE_IN_EXECUTION = 0x400;

  /**
   * Non-blocking ready condition.
   */
  public static final int EXECUTION_READY = STATE_INITIALIZED | STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE;

  /**
   * Non-blocking continue execution condition.
   */
  public static final int EXECUTION_CONTINUE = EXECUTION_READY | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION;

  /**
   * clean up the task, release resources, etc. This method should always called by the task executor thread(s).
   * 
   * @return if the cleanup succeeds without exceptions.
   * */
  private boolean cleanup() {
    // Only cleanup if initialized.
    if (AtomicUtils.unsetBitIfSetByValue(executionCondition, STATE_INITIALIZED)) {
      assert resourceManager.getExecutor().inTaskExecutor();
      try {
        root.close();
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception at operator close. Root operator: " + root + ".", ee);
        }
        taskExecutionFuture.setFailure(ee);
        return false;
      } finally {
        if (resourceManager != null) {
          resourceManager.cleanup();
        }
      }
      return true;
    }
    return true;
  }

  /**
   * Kill this task.
   * 
   * */
  void kill() {
    if (!AtomicUtils.setBitIfUnsetByValue(executionCondition, STATE_KILLED)) {
      return;
    }

    final Future<Void> executionHandleLocal = executionHandle;
    if (executionHandleLocal != null) {
      // Abruptly cancel the execution
      executionHandleLocal.cancel(true);
    }

    resourceManager.getExecutor().submit(executionTask);
  }

  /**
   * Execute this task.
   * */
  public void execute() {

    if (executionCondition.compareAndSet(EXECUTION_READY, EXECUTION_READY | STATE_EXECUTION_REQUESTED)) {
      // set in execution.
      executionHandle = resourceManager.getExecutor().submit(executionTask);
    }
  }

  /**
   * Task init future.
   * */
  private final DefaultTaskFuture initFuture = new DefaultTaskFuture(this, false);

  /**
   * @return check if init should be run.
   */
  private boolean shouldInit() {
    boolean setInit = (AtomicUtils.setBitIfUnsetByValue(executionCondition, STATE_INITIALIZED));
    if (!setInit) {
      // If already initialized
      return false;
    }
    // Check if already completed
    return (executionCondition.get() & (STATE_EOS | STATE_FAIL | STATE_KILLED)) == 0;
  }

  /**
   * Initialize the task. This method should always called by Non-task-execution threads.
   * 
   * @param execEnvVars execution environment variable.
   * @param resourceManager resource manager.
   * 
   * @return the future representing the init operations. The return of the init method does not mean the init
   *         operations are completed. The completeness should be checked through the future object
   * 
   * */
  public OperationFuture init(final TaskResourceManager resourceManager, final ImmutableMap<String, Object> execEnvVars) {
    try {
      if (shouldInit()) {
        assert !resourceManager.getExecutor().inTaskExecutor();
        this.resourceManager = resourceManager;
        try {
          ExecutableExecutionFuture<Void> initTask = new ExecutableExecutionFuture<Void>(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              synchronized (executionLock) {
                ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
                b.put(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER, resourceManager);
                b.putAll(execEnvVars);
                root.open(b.build());
                AtomicUtils.setBitByValue(executionCondition, STATE_INITIALIZED);
              }
              return null;
            }
          }, false);
          initTask.addPreListener(new OperationFutureListener() {
            @Override
            public void operationComplete(final OperationFuture future) {
              if (!future.isSuccess()) {
                initFuture.setFailure(future.getCause());
                if (LOGGER.isErrorEnabled()) {
                  LOGGER.error("Task failed to open because of exception:", future.getCause());
                }
                AtomicUtils.setBitByValue(executionCondition, STATE_FAIL);
                taskExecutionFuture.setFailure(future.getCause());
                cleanup();
              } else {
                initFuture.setSuccess();
              }
            }
          });
          resourceManager.getExecutor().submit(initTask);
        } catch (final CancellationException e) {
          // init is not cancelable, should never reach here
          initFuture.setFailure(e);
        }
      } else {
        initFuture.setFailure(new IllegalStateException("Already initialized"));
      }
    } catch (Throwable e) {
      initFuture.setFailure(e);
    }
    return initFuture;
  }

  /**
   * return owner query.
   * 
   * @return owner query.
   */
  public QueryPartition getOwnerQuery() {
    return ownerQuery;
  }

  /**
   * enable/disable output channels of the root(producer) of this task.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   * */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    if (root instanceof Producer) {
      ((Producer) root).updateChannelAvailability(workerId, enable);
    }
  }

  /**
   * return the root operator of this task.
   * 
   * @return the root operator
   */
  public RootOperator getRootOp() {
    return root;
  }

  /**
   * return the resource manager of this task.
   * 
   * @return the resource manager.
   */
  public TaskResourceManager getResourceManager() {
    return resourceManager;
  }
}
