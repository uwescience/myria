package edu.washington.escience.myriad.parallel;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myriad.util.AtomicUtils;
import edu.washington.escience.myriad.util.ReentrantSpinLock;

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
   * The root operator.
   * */
  private final RootOperator root;

  /**
   * The executor who is responsible for executing the task.
   * */
  private final ExecutorService myExecutor;

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
  private final Callable<Object> executionTask;

  /**
   * The output channels belonging to this task.
   * */
  private final StreamIOChannelID[] outputChannels;

  /**
   * The output channels belonging to this task.
   * */
  private final Map<StreamIOChannelID, Consumer> inputChannels;

  /**
   * The IDBInput operators in this task.
   * */
  private final Set<IDBInput> idbInputSet;

  /**
   * IPC ID of the owner {@link Worker} or {@link Server}.
   * */
  private final int ipcEntityID;

  /**
   * The handle for managing the execution of the task.
   * */
  private volatile Future<Object> executionHandle;

  /**
   * Protect the output channel status.
   */
  private final ReentrantSpinLock outputLock = new ReentrantSpinLock();

  /**
   * Future for the task execution.
   * */
  private final QueryFuture taskExecutionFuture;

  /**
   * The lock mainly to make operator memory consistency.
   * */
  private final Object executionLock = new Object();

  private volatile TaskResourceManager resourceManager;

  /**
   * @return the task execution future.
   */
  QueryFuture getExecutionFuture() {
    return taskExecutionFuture;
  }

  /**
   * @param ipcEntityID the IPC ID of the owner worker/master.
   * @param ownerQuery the owner query of this task.
   * @param root the root operator this task will run.
   * @param executor the executor who provides the execution service for the task to run on
   */
  QuerySubTreeTask(final int ipcEntityID, final QueryPartition ownerQuery, final RootOperator root,
      final ExecutorService executor) {
    this.ipcEntityID = ipcEntityID;
    executionCondition = new AtomicInteger(STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE);
    this.root = root;
    myExecutor = executor;
    this.ownerQuery = ownerQuery;
    taskExecutionFuture = new DefaultQueryFuture(this.ownerQuery, true);
    ((DefaultQueryFuture) taskExecutionFuture).setAttachment(this);
    idbInputSet = new HashSet<IDBInput>();
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
    // The return result means nothing here, just to make the Callable generic happy.
    executionTask = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        // synchronized to keep memory consistency
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Start task execution: " + QuerySubTreeTask.this);
        }
        try {
          synchronized (executionLock) {
            QuerySubTreeTask.this.executeActually();
          }
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
   * @return all the IDBInput operators in this task.
   * */
  Set<IDBInput> getIDBInputs() {
    return idbInputSet;
  }

  /**
   * gather all output (Producer or IDBInput's EOI report) channel IDs.
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
    } else if (currentOperator instanceof IDBInput) {
      IDBInput p = (IDBInput) currentOperator;
      ExchangePairID oID = p.getControllerOperatorID();
      int wID = p.getControllerWorkerID();
      outputExchangeChannels.add(new StreamIOChannelID(oID.getLong(), wID));
      idbInputSet.add(p);
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

    if (executionCondition.compareAndSet(EXECUTION_READY | STATE_EXECUTION_REQUESTED, EXECUTION_READY
        | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION)) {
      Throwable failureCause = null;
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

      if ((executionCondition.get() & STATE_FAIL) == STATE_FAIL) {
        // failed
        cleanup(true);
        taskExecutionFuture.setFailure(failureCause);
      } else if (root.eos()) {
        AtomicUtils.setBitByValue(executionCondition, STATE_EOS);
        cleanup(false);
        taskExecutionFuture.setSuccess();
      } else if ((executionCondition.get() & STATE_KILLED) == STATE_KILLED) {
        // killed
        cleanup(true);
        taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
      }
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
  private boolean isKilled() {
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
   * clean up the task, release resources, etc.
   * 
   * @param failed if the task execution is already failed.
   * */
  private void cleanup(final boolean failed) {
    if (AtomicUtils.unsetBitIfSetByValue(executionCondition, STATE_INITIALIZED)) {
      // Only cleanup if initialized.
      try {
        synchronized (executionLock) {
          root.close();
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception at operator close. Root operator: " + root + ".", ee);
        }
        if (!failed) {
          taskExecutionFuture.setFailure(ee);
        }
      } finally {
        if (resourceManager != null) {
          resourceManager.cleanup();
        }
      }
    }
  }

  /**
   * Kill this task.
   * 
   * */
  void kill() {
    if (isKilled()) {
      return;
    }
    while (!isKilled()) {

      int oldV = executionCondition.get();
      int notKilledInExec = (oldV & ~STATE_KILLED) | STATE_IN_EXECUTION;
      int killed = oldV | STATE_KILLED;
      if (executionCondition.compareAndSet(notKilledInExec, killed)) {
        // in execution, try to interrupt the execution thread, and let the execution thread to take care of the killing

        final Future<Object> executionHandleLocal = executionHandle;
        if (executionHandleLocal != null) {
          // Abruptly cancel the execution
          executionHandleLocal.cancel(true);
        }
      }
      oldV = executionCondition.get();
      int notKilledNotInExec = oldV & ~(STATE_KILLED | STATE_IN_EXECUTION);
      killed = oldV | STATE_KILLED;
      if (executionCondition.compareAndSet(notKilledNotInExec, killed)) {
        // not in execution, kill the query here
        cleanup(true);
        taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
      }
    }

  }

  /**
   * Execute this task.
   * */
  public void execute() {

    if (executionCondition.compareAndSet(EXECUTION_READY, EXECUTION_READY | STATE_EXECUTION_REQUESTED)) {
      // set in execution.
      executionHandle = myExecutor.submit(executionTask);
    }
  }

  /**
   * Initialize the task.
   * 
   * @param execEnvVars execution environment variable.
   * */
  public void init(final TaskResourceManager resourceManager, final ImmutableMap<String, Object> execEnvVars) {
    try {
      synchronized (executionLock) {
        ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
        b.put(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER, resourceManager);
        b.putAll(execEnvVars);
        this.resourceManager = resourceManager;
        root.open(b.build());
      }
      AtomicUtils.setBitByValue(executionCondition, STATE_INITIALIZED);
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Task failed to open because of execption. ", e);
      }
      AtomicUtils.setBitByValue(executionCondition, STATE_FAIL);
      cleanup(true);
      taskExecutionFuture.setFailure(e);
    }
  }
}
