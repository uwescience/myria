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

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
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
final class QuerySubTreeTask {
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
   * Denoting if currently the task is in blocking execution by the executor.
   * */
  private volatile boolean inBlockingExecution;

  /**
   * Each bit for each output channel. Currently, if a single output channel is not writable, the whole task stops.
   * */
  private final BitSet outputChannelAvailable;

  /**
   * the owner query partition.
   * */
  private final QueryPartition ownerQuery;

  /**
   * Non-blocking execution task.
   * */
  private final Callable<Object> nonBlockingExecutionTask;

  /**
   * Blocking execution task.
   * */
  private final Callable<Object> blockingExecutionTask;

  /**
   * Current execution mode.
   * */
  private final QueryExecutionMode executionMode;

  /**
   * The output channels belonging to this task.
   * */
  private final ExchangeChannelID[] outputChannels;

  /**
   * The output channels belonging to this task.
   * */
  private final Map<ExchangeChannelID, Consumer> inputChannels;

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
   * @param executionMode blocking/nonblocking mode.
   */
  QuerySubTreeTask(final int ipcEntityID, final QueryPartition ownerQuery, final RootOperator root,
      final ExecutorService executor, final QueryExecutionMode executionMode) {
    this.ipcEntityID = ipcEntityID;
    this.executionMode = executionMode;
    if (this.executionMode == QueryExecutionMode.NON_BLOCKING) {
      nonBlockingExecutionCondition =
          new AtomicInteger(STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE | STATE_NOT_PAUSED | STATE_NOT_KILLED
              | STATE_NOT_EOS | STATE_EXECUTION_MODE_NON_BLOCKING);
    } else {
      nonBlockingExecutionCondition =
          new AtomicInteger(STATE_OUTPUT_AVAILABLE | STATE_INPUT_AVAILABLE | STATE_NOT_PAUSED | STATE_NOT_KILLED
              | STATE_NOT_EOS);
    }
    this.root = root;
    myExecutor = executor;
    this.ownerQuery = ownerQuery;
    taskExecutionFuture = new DefaultQueryFuture(this.ownerQuery, true);
    ((DefaultQueryFuture) taskExecutionFuture).setAttachment(this);
    idbInputSet = new HashSet<IDBInput>();
    HashSet<ExchangeChannelID> outputChannelSet = new HashSet<ExchangeChannelID>();
    collectDownChannels(root, outputChannelSet);
    outputChannels = outputChannelSet.toArray(new ExchangeChannelID[] {});
    HashMap<ExchangeChannelID, Consumer> inputChannelMap = new HashMap<ExchangeChannelID, Consumer>();
    collectUpChannels(root, inputChannelMap);
    inputChannels = inputChannelMap;
    Arrays.sort(outputChannels);
    outputChannelAvailable = new BitSet(outputChannels.length);
    for (int i = 0; i < outputChannels.length; i++) {
      outputChannelAvailable.set(i);
    }
    inBlockingExecution = false;
    // The return result means nothing here, just to make the Callable generic happy.
    nonBlockingExecutionTask = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        // synchronized to keep memory consistency
        synchronized (executionLock) {
          try {
            QuerySubTreeTask.this.executeNonBlocking();
          } finally {
            executionHandle = null;
          }
          return null;
        }
      }
    };

    blockingExecutionTask = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        QuerySubTreeTask.this.executeBlocking();
        return null;
      }
    };
  }

  /**
   * @return all input channels belonging to this task.
   * */
  Map<ExchangeChannelID, Consumer> getInputChannels() {
    return inputChannels;
  }

  /**
   * @return all output channels belonging to this task.
   */
  ExchangeChannelID[] getOutputChannels() {
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
      final HashSet<ExchangeChannelID> outputExchangeChannels) {

    if (currentOperator instanceof Producer) {
      Producer p = (Producer) currentOperator;
      p.getDestinationWorkerIDs(ipcEntityID);
      ExchangePairID[] oIDs = p.operatorIDs();
      int[] destWorkers = p.getDestinationWorkerIDs(ipcEntityID);
      for (int i = 0; i < destWorkers.length; i++) {
        outputExchangeChannels.add(new ExchangeChannelID(oIDs[i].getLong(), destWorkers[i]));
      }
    } else if (currentOperator instanceof IDBInput) {
      IDBInput p = (IDBInput) currentOperator;
      ExchangePairID oID = p.getControllerOperatorID();
      int wID = p.getControllerWorkerID();
      outputExchangeChannels.add(new ExchangeChannelID(oID.getLong(), wID));
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
      final Map<ExchangeChannelID, Consumer> inputExchangeChannels) {

    if (currentOperator instanceof Consumer) {
      Consumer c = (Consumer) currentOperator;
      int[] sourceWorkers = c.getSourceWorkers(ipcEntityID);
      ExchangePairID oID = c.getOperatorID();
      ConsumerChannel[] ccs = new ConsumerChannel[sourceWorkers.length];
      int i = 0;
      for (int sourceWorker : sourceWorkers) {
        inputExchangeChannels.put(new ExchangeChannelID(oID.getLong(), sourceWorker), c);
        ccs[i++] = new ConsumerChannel(this, c, sourceWorker);
      }
      c.setExchangeChannels(ccs);
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
    setInputAvailable();
    nonBlockingExecute();
  }

  /**
   * Called by Netty downstream IO worker threads.
   * 
   * @param outputChannelID the logical output channel ID.
   */
  public void notifyOutputDisabled(final ExchangeChannelID outputChannelID) {
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
    setOutputDisabled();
  }

  /**
   * Called by Netty downstream IO worker threads.
   * 
   * @param outputChannelID the down channel ID.
   * */
  public void notifyOutputEnabled(final ExchangeChannelID outputChannelID) {
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
            setOutputAvailable();
            nonBlockingExecute();
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
    int state = nonBlockingExecutionCondition.get();
    String splitter = "";
    if ((state & STATE_EXECUTION_MODE_NON_BLOCKING) == STATE_EXECUTION_MODE_NON_BLOCKING) {
      stateS.append(splitter + "Non_Blocking");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_INITIALIZED) == STATE_INITIALIZED) {
      stateS.append(splitter + "Initialized");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_INPUT_AVAILABLE) == STATE_INPUT_AVAILABLE) {
      stateS.append(splitter + "Input_Available");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_NOT_EOS) != STATE_NOT_EOS) {
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
    if ((state & QuerySubTreeTask.STATE_NOT_KILLED) != STATE_NOT_KILLED) {
      stateS.append(splitter + "Killed");
      splitter = " | ";
    }
    if ((state & QuerySubTreeTask.STATE_NOT_PAUSED) != STATE_NOT_PAUSED) {
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
   * Execute this task in non-blocking mode.
   * 
   * @return if the task is EOS.
   * */
  private Object executeNonBlocking() {

    if (nonBlockingExecutionCondition.compareAndSet(NON_BLOCKING_EXECUTION_READY | STATE_EXECUTION_REQUESTED,
        NON_BLOCKING_EXECUTION_READY | STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION)) {
      Throwable failureCause = null;
      NON_BLOCKING_EXECUTE : while (true) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
          }
          AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, STATE_INTERRUPTED);

          // TODO clean up task state
        } else if (ownerQuery.isPaused()) {
          // the owner query is paused, exit execution.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution paused because the query is paused. Root operator: {}.", root);
          }
        } else {
          // do the execution

          clearInput(); // clear input at beginning.

          try {
            root.nextReady();
          } catch (final Throwable e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Unexpected exception occur at operator excution. Operator: " + root, e);
            }
            failureCause = e;
            AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, STATE_FAIL);
          }

        }

        // Check if another round of execution is needed.
        int oldV = nonBlockingExecutionCondition.get();
        while (oldV != NON_BLOCKING_EXECUTION_CONTINUE) {
          // try clear the STATE_EXECUTION_REQUESTED and STATE_IN_EXECUTION bit
          if (nonBlockingExecutionCondition.compareAndSet(oldV, oldV
              & ~(STATE_EXECUTION_REQUESTED | STATE_IN_EXECUTION))) {
            // exit execution.
            break NON_BLOCKING_EXECUTE;
          }
          oldV = nonBlockingExecutionCondition.get();
        }

      }

      // clear interrupted
      AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~STATE_INTERRUPTED);

      if ((nonBlockingExecutionCondition.get() & STATE_FAIL) == STATE_FAIL) {
        // failed
        taskExecutionFuture.setFailure(failureCause);
      } else if (root.eos()) {
        setEOS();
        taskExecutionFuture.setSuccess();
      } else if ((nonBlockingExecutionCondition.get() & STATE_NOT_KILLED) != STATE_NOT_KILLED) {
        // killed
        taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
      }
    }
    return null;
  }

  /**
   * Current non-blocking execution condition.
   * */
  private final AtomicInteger nonBlockingExecutionCondition;

  /**
   * The task is initialized.
   */
  private static final int STATE_INITIALIZED = 0x01;

  /**
   * Mark the task state as initialized.
   * */
  private void setInitialized() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, STATE_INITIALIZED);
  }

  /**
   * All output of the task are available.
   */
  private static final int STATE_OUTPUT_AVAILABLE = 0x02;

  /**
   * Mark the output is available.
   * */
  private void setOutputAvailable() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, STATE_OUTPUT_AVAILABLE);
  }

  /**
   * Mark the output is disabled.
   * */
  private void setOutputDisabled() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~STATE_OUTPUT_AVAILABLE);
  }

  /**
   * @return if the output channels are available for writing.
   * */
  private boolean isOutputAvailable() {
    return (nonBlockingExecutionCondition.get() & STATE_OUTPUT_AVAILABLE) == STATE_OUTPUT_AVAILABLE;
  }

  /**
   * The current execution mode is non-blocking.
   */
  private static final int STATE_EXECUTION_MODE_NON_BLOCKING = 0x04;

  /**
   * Any input of the task is available.
   */
  private static final int STATE_INPUT_AVAILABLE = 0x08;

  /**
   * Mark input available.
   * */
  private void setInputAvailable() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, STATE_INPUT_AVAILABLE);
  }

  /**
   * Clear the input available bit.
   * */
  private void clearInput() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~STATE_INPUT_AVAILABLE);
  }

  /**
   * The task is paused.
   */
  private static final int STATE_NOT_PAUSED = 0x10;

  /**
   * The task is killed.
   */
  private static final int STATE_NOT_KILLED = 0x20;

  /**
   * Change the task's state to be killed.
   * */
  private void setKilled() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~STATE_NOT_KILLED);
  }

  /**
   * @return if the task is already get killed.
   * */
  private boolean isKilled() {
    return (nonBlockingExecutionCondition.get() & STATE_NOT_KILLED) == 0;
  }

  /**
   * The task is not EOS.
   * */
  private static final int STATE_NOT_EOS = 0x40;

  /**
   * Change the task's state to be EOS.
   * */
  private void setEOS() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~STATE_NOT_EOS);
  }

  /**
   * @return if the task is already EOS.
   * */
  private boolean isEOS() {
    return (nonBlockingExecutionCondition.get() & STATE_NOT_EOS) == 0;
  }

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
  public static final int NON_BLOCKING_EXECUTION_READY = STATE_INITIALIZED | STATE_OUTPUT_AVAILABLE
      | STATE_EXECUTION_MODE_NON_BLOCKING | STATE_INPUT_AVAILABLE | STATE_NOT_PAUSED | STATE_NOT_KILLED | STATE_NOT_EOS;

  /**
   * Non-blocking continue.
   */
  public static final int NON_BLOCKING_EXECUTION_CONTINUE = NON_BLOCKING_EXECUTION_READY | STATE_EXECUTION_REQUESTED
      | STATE_IN_EXECUTION;

  /**
   * @return if blocking execution is ready.
   * */
  private boolean readyForBlockingExecution() {
    // return initialized && !inBlockingExecution && executionMode == QueryExecutionMode.BLOCKING
    // && !ownerQuery.isPaused() && !ownerQuery.isKilled();
    return false;
  }

  /**
   * Execute this query sub-tree task under blocking mode.
   * 
   * @return true if successfully executed and the root is EOS.
   * */
  private Object executeBlocking() {
    try {

      synchronized (executionLock) {
        while (!root.eos()) {
          if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
            }
            taskExecutionFuture.setFailure(new InterruptedException("Task interrupted."));
            break;
          }
          root.next();
        }
      }

      taskExecutionFuture.setSuccess();

      return true;
    } catch (InterruptedException ee) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Execution interrupted. Exit directly. ");
      }
      taskExecutionFuture.setFailure(ee);
      return false;
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unexpected exception occur at operator excution, close directly. Operator: " + root, e);
      }
      taskExecutionFuture.setFailure(e);
    } finally {
      inBlockingExecution = false;
    }
    return null;
  }

  /**
   * clean up the task, release resources, etc.
   * */
  public void cleanup() {
    if (AtomicUtils.unsetBitIfSet(nonBlockingExecutionCondition, Integer.numberOfTrailingZeros(STATE_INITIALIZED))) {
      // Only cleanup if initialized.
      try {
        synchronized (executionLock) {
          root.close();
        }
      } catch (DbException ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception at operator close. Root operator: " + root + ".", ee);
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

      int oldV = nonBlockingExecutionCondition.get();
      int notKilledInExec = oldV | STATE_NOT_KILLED | STATE_IN_EXECUTION;
      int killed = oldV & (~STATE_NOT_KILLED);
      if (nonBlockingExecutionCondition.compareAndSet(notKilledInExec, killed)) {
        // in execution, try to interrupt the execution thread, and let the execution thread to take care of the killing

        final Future<Object> executionHandleLocal = executionHandle;
        if (executionHandleLocal != null) {
          // Abruptly cancel the execution
          executionHandleLocal.cancel(true);
        }
      }
      oldV = nonBlockingExecutionCondition.get();
      int notKilledNotInExec = oldV | STATE_NOT_KILLED & ~STATE_IN_EXECUTION;
      killed = oldV & (~STATE_NOT_KILLED);
      if (nonBlockingExecutionCondition.compareAndSet(notKilledNotInExec, killed)) {
        // not in execution, kill the query here
        taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
      }
    }

  }

  /**
   * Execute this task in non-blocking mode.
   * */
  public void nonBlockingExecute() {

    if (nonBlockingExecutionCondition.compareAndSet(NON_BLOCKING_EXECUTION_READY, NON_BLOCKING_EXECUTION_READY
        | STATE_EXECUTION_REQUESTED)) {
      // set in execution.
      executionHandle = myExecutor.submit(nonBlockingExecutionTask);
    }
  }

  /**
   * Execute this task in blocking mode.
   * */
  public void blockingExecute() {
    if (readyForBlockingExecution()) {
      inBlockingExecution = true;
      myExecutor.submit(blockingExecutionTask);
    }
  }

  /**
   * Initialize the task.
   * 
   * @param execUnitEnv execution environment variable.s
   * */
  public void init(final ImmutableMap<String, Object> execUnitEnv) {
    try {
      synchronized (executionLock) {
        root.open(execUnitEnv);
      }
      setInitialized();
    } catch (DbException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Query failed to open. Close the query directly.", e);
      }
      taskExecutionFuture.setFailure(e);
    }
  }

}
