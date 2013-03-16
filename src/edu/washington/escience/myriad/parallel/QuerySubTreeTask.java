package edu.washington.escience.myriad.parallel;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;

/**
 * Non-blocking driving code of a sub-query.
 * */
final class QuerySubTreeTask {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QuerySubTreeTask.class.getName());

  /**
   * The root operator.
   * */
  final RootOperator root;

  /**
   * Num input TupleBatch from consumer operators.
   * */
  final AtomicLong numInputTBs;

  /**
   * The executor who is responsible for executing the task.
   * */
  final ExecutorService myExecutor;

  /**
   * Denoting if currently the task is in blocking execution by the executor.
   * */
  volatile boolean inBlockingExecution;

  /**
   * if the task is initialized.
   * */
  volatile boolean initialized;

  /**
   * There are available output slots.
   * */
  volatile boolean outputAvailable;

  /**
   * Each bit for each output channel. Currently, if a single output channel is not writable, the whole task stops.
   * */
  final BitSet outputChannelAvailable;

  /**
   * the owner query partition.
   * */
  final QueryPartition ownerQuery;

  /**
   * Non-blocking execution task.
   * */
  final Callable<Boolean> nonBlockingExecutionTask;

  /**
   * Blocking execution task.
   * */
  final Callable<Boolean> blockingExecutionTask;

  /**
   * Current execution mode.
   * */
  final QueryExecutionMode executionMode;

  /**
   * The output channels belonging to this task.
   * */
  final ExchangeChannelID[] outputChannels;

  /**
   * The output channels belonging to this task.
   * */
  final ExchangeChannelID[] inputChannels;

  /**
   * IPC ID of the owner {@link Worker} or {@link Server}.
   * */
  final int ipcEntityID;

  QuerySubTreeTask(final int ipcEntityID, final QueryPartition ownerQuery, final RootOperator root,
      final ExecutorService executor, final QueryExecutionMode executionMode) {
    this.ipcEntityID = ipcEntityID;
    this.executionMode = executionMode;
    this.root = root;
    myExecutor = executor;
    this.ownerQuery = ownerQuery;
    outputAvailable = true;
    HashSet<ExchangeChannelID> outputChannelSet = new HashSet<ExchangeChannelID>();
    collectDownChannels(root, outputChannelSet);
    outputChannels = outputChannelSet.toArray(new ExchangeChannelID[] {});
    HashSet<ExchangeChannelID> inputChannelSet = new HashSet<ExchangeChannelID>();
    collectUpChannels(root, inputChannelSet);
    inputChannels = inputChannelSet.toArray(new ExchangeChannelID[] {});
    Arrays.sort(outputChannels);
    outputChannelAvailable = new BitSet(outputChannels.length);
    for (int i = 0; i < outputChannels.length; i++) {
      outputChannelAvailable.set(i);
    }
    numInputTBs = new AtomicLong(1); // Set 1 to cheat the Worker to execute each newly created task.
    inBlockingExecution = false;
    initialized = false;
    nonBlockingExecutionTask = new Callable<Boolean>() {
      @Override
      public synchronized Boolean call() throws Exception {
        // synchronized to keep memory consistency
        return QuerySubTreeTask.this.executeNonBlocking();
      }
    };
    blockingExecutionTask = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return QuerySubTreeTask.this.executeBlocking();
      }
    };
  }

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

  private void collectUpChannels(final Operator currentOperator, final HashSet<ExchangeChannelID> inputExchangeChannels) {

    if (currentOperator instanceof Consumer) {
      Consumer c = (Consumer) currentOperator;
      int[] sourceWorkers = c.getSourceWorkers(ipcEntityID);
      ExchangePairID oID = c.getOperatorID();
      for (int sourceWorker : sourceWorkers) {
        inputExchangeChannels.add(new ExchangeChannelID(oID.getLong(), sourceWorker));
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
   * */
  public void notifyNewInput() {
    numInputTBs.incrementAndGet();
    nonBlockingExecute();
  }

  /**
   * Called by Netty downstream IO worker threads.
   * */
  public void notifyOutputDisabled(final ExchangeChannelID outputChannelID) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Output disabled: " + outputChannelID);
    }
    int idx = Arrays.binarySearch(outputChannels, outputChannelID);
    outputChannelAvailable.clear(idx);
    outputAvailable = false; // output not available if any of the output channels is not available
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
    if (!outputAvailable) {
      int idx = Arrays.binarySearch(outputChannels, outputChannelID);
      if (!outputChannelAvailable.get(idx)) {
        outputChannelAvailable.set(idx);
        if (outputChannelAvailable.nextClearBit(0) >= outputChannels.length) {
          outputAvailable = true;
          nonBlockingExecute();
        }
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
      if (root.eos()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Root operator already EOS. Root operator: " + root + ". Quit directly.");
        }
        return true;
      }

      while (true) {
        if (Thread.interrupted()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
          }
          cleanup();
          break;
        }
        long atStart = numInputTBs.get();

        root.nextReady();

        if (root.eos()) {
          break;
        }

        if (numInputTBs.compareAndSet(atStart, 0)) {
          // no new input TB.
          break;
        }
      }
      if (root.eos()) {
        ownerQuery.taskFinish(this);
      }

      return root.eos();
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unexpected exception occur at operator excution, close directly. Operator: " + root, e);
      }
      cleanup();
    }
    return true;
  }

  /**
   * This method is for nonblocking execution mode only.<br>
   * 1. There are newly arriving input data queued since last execution. <br>
   * 2. All the output queues have available slots. <br>
   * 3. The execution task is currently not in execution.
   * 
   * @return if the nonblocking task is ready for execution.
   * */
  public boolean readyForNonBlockingExecution() {
    return initialized && outputAvailable && executionMode == QueryExecutionMode.NON_BLOCKING && numInputTBs.get() > 0;
  }

  boolean readyForBlockingExecution() {
    return initialized && !inBlockingExecution && executionMode == QueryExecutionMode.BLOCKING;
  }

  /**
   * Execute this query sub-tree task under blocking mode.
   * 
   * @return true if successfully executed and the root is EOS.
   * */
  private Boolean executeBlocking() {
    try {
      while (!root.eos()) {
        if (Thread.interrupted()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Operator task execution interrupted. Root operator: " + root + ". Close directly.");
          }
          cleanup();
          break;
        }
        root.next();
      }

      ownerQuery.taskFinish(this);

      return true;
    } catch (InterruptedException ee) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Execution interrupted. Exit directly. ");
      }
      cleanup();
      return false;
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unexpected exception occur at operator excution, close directly. Operator: " + root, e);
      }
      cleanup();
    } finally {
      inBlockingExecution = false;
    }
    return true;
  }

  public void cleanup() {
    try {
      root.close();
      initialized = false;
    } catch (DbException ee) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unknown exception at operator close. Root operator: " + root + ".", ee);
      }
    }
  }

  /**
   * Execute this task in non-blocking mode.
   * */
  public void nonBlockingExecute() {
    if (readyForNonBlockingExecution()) {
      myExecutor.submit(nonBlockingExecutionTask);
    }
  }

  /**
   * Execute this task in blocking mode.
   * */
  public void blockingExecute() {
    if (readyForNonBlockingExecution()) {
      inBlockingExecution = true;
      myExecutor.submit(blockingExecutionTask);
    }
  }

  public void init(final ImmutableMap<String, Object> execUnitEnv) {
    try {
      root.open(execUnitEnv);
      initialized = true;
    } catch (DbException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Query failed to open. Close the query directly.", e);
      }
      cleanup();
    }
  }

}
