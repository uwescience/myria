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

import org.apache.commons.lang3.builder.ToStringBuilder;

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
  private final Callable<Boolean> nonBlockingExecutionTask;

  /**
   * Blocking execution task.
   * */
  private final Callable<Boolean> blockingExecutionTask;

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
  private volatile Future<Boolean> executionHandle;

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
          new AtomicInteger(OUTPUT_AVAILABLE | INPUT_AVAILABLE | NOT_PAUSED | NOT_KILLED | NOT_EOS | NOT_IN_EXECUTION
              | EXECUTION_MODE_NON_BLOCKING);
    } else {
      nonBlockingExecutionCondition =
          new AtomicInteger(OUTPUT_AVAILABLE | INPUT_AVAILABLE | NOT_PAUSED | NOT_KILLED | NOT_EOS | NOT_IN_EXECUTION);
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
    nonBlockingExecutionTask = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // synchronized to keep memory consistency
        synchronized (executionLock) {
          try {
            return QuerySubTreeTask.this.executeNonBlocking();
          } finally {
            executionHandle = null;
            if (isKilled()) {
              taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
            } else {
              if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                // Normally they should be killed tasks
                taskExecutionFuture.setFailure(new InterruptedException("Task gets interrupted"));
              }
            }
          }
        }
      }
    };

    blockingExecutionTask = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return QuerySubTreeTask.this.executeBlocking();
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
    return isEOS() || isKilled();
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
    return ToStringBuilder.reflectionToString(this);
  }

  /**
   * Execute this task in non-blocking mode.
   * 
   * @return if the task is EOS.
   * */
  private Boolean executeNonBlocking() {
    try {

      NON_BLOCKING_EXECUTE : while (true) {
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
          }

          break;
        }

        if (ownerQuery.isPaused()) {
          // the owner query is paused, exit execution.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution paused because the query is paused. Root operator: {}.", root);
          }
          break;
        }

        clearInput(); // clear input at beginning.

        root.nextReady();

        if (root.eos()) {
          setEOS();
          taskExecutionFuture.setSuccess();
        }

        // Check if another round of execution is needed.
        int oldV = nonBlockingExecutionCondition.get();
        while ((oldV & NON_BLOCKING_EXECUTION_CONTINUE) != NON_BLOCKING_EXECUTION_CONTINUE) {
          if (nonBlockingExecutionCondition.compareAndSet(oldV, oldV | NOT_IN_EXECUTION)) {
            // exit execution.
            break NON_BLOCKING_EXECUTE;
          }
          oldV = nonBlockingExecutionCondition.get();
        }
      }

      return root.eos();
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unexpected exception occur at operator excution. Operator: " + root, e);
      }
      taskExecutionFuture.setFailure(e);
    }
    return true;
  }

  /**
   * Current non-blocking execution condition.
   * */
  private final AtomicInteger nonBlockingExecutionCondition;

  /**
   * The task is initialized.
   */
  private static final int INITIALIZED = 0x01;

  /**
   * Mark the task state as initialized.
   * */
  private void setInitialized() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, INITIALIZED);
  }

  /**
   * All output of the task are available.
   */
  private static final int OUTPUT_AVAILABLE = 0x02;

  /**
   * Mark the output is available.
   * */
  private void setOutputAvailable() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, OUTPUT_AVAILABLE);
  }

  /**
   * Mark the output is disabled.
   * */
  private void setOutputDisabled() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~OUTPUT_AVAILABLE);
  }

  /**
   * @return if the output channels are available for writing.
   * */
  private boolean isOutputAvailable() {
    return (nonBlockingExecutionCondition.get() & OUTPUT_AVAILABLE) == OUTPUT_AVAILABLE;
  }

  /**
   * The current execution mode is non-blocking.
   */
  private static final int EXECUTION_MODE_NON_BLOCKING = 0x04;

  /**
   * Any input of the task is available.
   */
  private static final int INPUT_AVAILABLE = 0x08;

  /**
   * Mark input available.
   * */
  private void setInputAvailable() {
    AtomicUtils.bitwiseOrAndGet(nonBlockingExecutionCondition, INPUT_AVAILABLE);
  }

  /**
   * Clear the input available bit.
   * */
  private void clearInput() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~INPUT_AVAILABLE);
  }

  /**
   * The task is paused.
   */
  private static final int NOT_PAUSED = 0x10;

  /**
   * The task is killed.
   */
  private static final int NOT_KILLED = 0x20;

  /**
   * Change the task's state to be killed.
   * */
  private void setKilled() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~NOT_KILLED);
  }

  /**
   * @return if the task is already get killed.
   * */
  private boolean isKilled() {
    return (nonBlockingExecutionCondition.get() & NOT_KILLED) == 0;
  }

  /**
   * The task is not EOS.
   * */
  private static final int NOT_EOS = 0x40;

  /**
   * Change the task's state to be EOS.
   * */
  private void setEOS() {
    AtomicUtils.bitwiseAndAndGet(nonBlockingExecutionCondition, ~NOT_EOS);
  }

  /**
   * @return if the task is already EOS.
   * */
  private boolean isEOS() {
    return (nonBlockingExecutionCondition.get() & NOT_EOS) == 0;
  }

  /**
   * The task is currently not in execution.
   * */
  private static final int NOT_IN_EXECUTION = 0x80;

  /**
   * Non-blocking ready condition.
   */
  public static final int NON_BLOCKING_EXECUTION_READY = INITIALIZED | OUTPUT_AVAILABLE | EXECUTION_MODE_NON_BLOCKING
      | INPUT_AVAILABLE | NOT_PAUSED | NOT_KILLED | NOT_EOS | NOT_IN_EXECUTION;

  /**
   * Non-blocking continue.
   */
  public static final int NON_BLOCKING_EXECUTION_CONTINUE = NON_BLOCKING_EXECUTION_READY & ~NOT_IN_EXECUTION;

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
  private Boolean executeBlocking() {
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
    return true;
  }

  /**
   * clean up the task, release resources, etc.
   * */
  public void cleanup() {
    if (AtomicUtils.unsetBitIfSet(nonBlockingExecutionCondition, Integer.numberOfTrailingZeros(INITIALIZED))) {
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
    setKilled();
    final Future<Boolean> executionHandleLocal = executionHandle;
    if (executionHandleLocal != null) {
      // Abruptly cancel the execution
      executionHandleLocal.cancel(true);
    } else {
      taskExecutionFuture.setFailure(new QueryKilledException("Task gets killed"));
    }
  }

  /**
   * Execute this task in non-blocking mode.
   * */
  public void nonBlockingExecute() {
    int oldV = nonBlockingExecutionCondition.get();
    if ((oldV & NON_BLOCKING_EXECUTION_READY) == NON_BLOCKING_EXECUTION_READY) {
      if (nonBlockingExecutionCondition.compareAndSet(oldV, oldV & ~NOT_IN_EXECUTION)) {
        // set in execution.
        executionHandle = myExecutor.submit(nonBlockingExecutionTask);
        if (isKilled()) {
          // check if killed because the kill may happen right between the NON_IN_EXECUTION bit is unset and the
          // executionHandle is set.
          executionHandle.cancel(true);
        }
      }
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
