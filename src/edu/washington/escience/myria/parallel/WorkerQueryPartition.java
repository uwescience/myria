package edu.washington.escience.myria.parallel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * A {@link WorkerQueryPartition} is a partition of a query plan at a single worker.
 * */
public class WorkerQueryPartition implements QueryPartition {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryPartition.class);

  /**
   * The query ID.
   * */
  private final long queryID;

  /**
   * All tasks.
   * */
  private final Set<QuerySubTreeTask> tasks;

  /**
   * Number of finished tasks.
   * */
  private final AtomicInteger numFinishedTasks;

  /**
   * The owner {@link Worker}.
   * */
  private final Worker ownerWorker;

  /**
   * The ftMode.
   * */
  private final FTMODE ftMode;

  /**
   * The profiling mode.
   */
  private final boolean profilingMode;

  /**
   * priority, currently no use.
   * */
  private volatile int priority;

  /**
   * Store the current pause future if the query is in pause, otherwise null.
   * */
  private final AtomicReference<QueryFuture> pauseFuture = new AtomicReference<QueryFuture>(null);

  /**
   * the future for the query's execution.
   * */
  private final DefaultQueryFuture executionFuture = new DefaultQueryFuture(this, true);

  /**
   * record all failed tasks.
   * */
  private final ConcurrentLinkedQueue<QuerySubTreeTask> failTasks = new ConcurrentLinkedQueue<QuerySubTreeTask>();

  /**
   * Current alive worker set.
   * */
  private final Set<Integer> missingWorkers;

  /**
   * The future listener for processing the complete events of the execution of all the query's tasks.
   * */
  private final TaskFutureListener taskExecutionListener = new TaskFutureListener() {

    @Override
    public void operationComplete(final TaskFuture future) throws Exception {
      QuerySubTreeTask drivingTask = future.getTask();
      int currentNumFinished = numFinishedTasks.incrementAndGet();

      executionFuture.setProgress(1, currentNumFinished, tasks.size());
      Throwable failureReason = future.getCause();
      if (!future.isSuccess()) {
        failTasks.add(drivingTask);
        if (!(failureReason instanceof QueryKilledException)) {
          // The task is a failure, not killed.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("got a failed task, root op = " + drivingTask.getRootOp().getOpName() + ", cause ",
                failureReason);
          }
          for (QuerySubTreeTask t : tasks) {
            // kill other tasks
            t.kill();
          }
        }
      }

      if (currentNumFinished >= tasks.size()) {
        queryStatistics.markQueryEnd();
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Query #" + queryID + " executed for "
              + DateTimeUtils.nanoElapseToHumanReadable(queryStatistics.getQueryExecutionElapse()));
        }
        if (failTasks.isEmpty()) {
          executionFuture.setSuccess();
        } else {
          Throwable existingCause = executionFuture.getCause();
          Throwable newCause = failTasks.peek().getExecutionFuture().getCause();
          if (existingCause == null) {
            executionFuture.setFailure(newCause);
          } else {
            existingCause.addSuppressed(newCause);
          }
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("New finished task: {}. {} remain.", drivingTask, (tasks.size() - currentNumFinished));
        }
      }
    }

  };

  /***
   * Statistics of this query partition.
   */
  private final QueryExecutionStatistics queryStatistics = new QueryExecutionStatistics();

  /**
   * @param plan the plan of this query partition.
   * @param queryID the id of the query.
   * @param ownerWorker the worker on which this query partition is going to run
   * */
  public WorkerQueryPartition(final SingleQueryPlanWithArgs plan, final long queryID, final Worker ownerWorker) {
    this.queryID = queryID;
    ftMode = plan.getFTMode();
    profilingMode = plan.isProfilingMode();
    List<RootOperator> operators = plan.getRootOps();
    tasks = new HashSet<QuerySubTreeTask>(operators.size());
    numFinishedTasks = new AtomicInteger(0);
    this.ownerWorker = ownerWorker;
    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
    for (final RootOperator taskRootOp : operators) {
      createTask(taskRootOp);
    }
  }

  /**
   * create a task.
   * 
   * @param root the root operator of this task.
   * @return the task.
   */
  public QuerySubTreeTask createTask(final RootOperator root) {
    final QuerySubTreeTask drivingTask =
        new QuerySubTreeTask(ownerWorker.getIPCConnectionPool().getMyIPCID(), this, root, ownerWorker
            .getQueryExecutor());
    TaskFuture taskExecutionFuture = drivingTask.getExecutionFuture();
    taskExecutionFuture.addListener(taskExecutionListener);

    tasks.add(drivingTask);

    HashSet<Consumer> consumerSet = new HashSet<Consumer>();
    consumerSet.addAll(drivingTask.getInputChannels().values());
    for (final Consumer c : consumerSet) {
      FlowControlBagInputBuffer<TupleBatch> inputBuffer =
          new FlowControlBagInputBuffer<TupleBatch>(ownerWorker.getIPCConnectionPool(), c
              .getInputChannelIDs(ownerWorker.getIPCConnectionPool().getMyIPCID()), ownerWorker
              .getInputBufferCapacity(), ownerWorker.getInputBufferRecoverTrigger(), ownerWorker.getIPCConnectionPool());
      inputBuffer.addListener(FlowControlBagInputBuffer.NEW_INPUT_DATA, new IPCEventListener() {
        @Override
        public void triggered(final IPCEvent event) {
          drivingTask.notifyNewInput();
        }
      });
      c.setInputBuffer(inputBuffer);
    }

    return drivingTask;
  }

  /**
   * @return the future for the query's execution.
   * */
  final QueryFuture getExecutionFuture() {
    return executionFuture;
  }

  @Override
  public final void init() {
    for (QuerySubTreeTask t : tasks) {
      init(t);
    }
  }

  /**
   * initialize a task.
   * 
   * @param t the task
   * */
  public final void init(final QuerySubTreeTask t) {
    TaskResourceManager resourceManager =
        new TaskResourceManager(ownerWorker.getIPCConnectionPool(), t, ownerWorker.getQueryExecutionMode());
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    t.init(resourceManager, b.putAll(ownerWorker.getExecEnvVars()).build());
  }

  @Override
  public final long getQueryID() {
    return queryID;
  }

  @Override
  public final int compareTo(final QueryPartition o) {
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
    return tasks + ", priority:" + priority;
  }

  @Override
  public final void startExecution() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query : " + getQueryID() + " start processing.");
    }
    queryStatistics.markQueryStart();
    for (QuerySubTreeTask t : tasks) {
      t.execute();
    }
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  /**
   * Pause the worker query partition. If the query is currently paused, do nothing.
   * 
   * @return the future instance of the pause action. The future will be set as done if and only if all the tasks in
   *         this query have stopped execution. During a pause of the query, all call to this method returns the same
   *         future instance. Two pause calls when the query is not paused at either of the calls return two different
   *         instances.
   * */
  @Override
  public final QueryFuture pause() {
    final QueryFuture pauseF = new DefaultQueryFuture(this, true);
    while (!pauseFuture.compareAndSet(null, pauseF)) {
      QueryFuture current = pauseFuture.get();
      if (current != null) {
        // already paused by some other threads, do not do the actual pause
        return current;
      }
    }
    return pauseF;
  }

  /**
   * Resume the worker query partition.
   * 
   * @return the future instance of the resume action.
   * */
  @Override
  public final QueryFuture resume() {
    QueryFuture pf = pauseFuture.getAndSet(null);
    DefaultQueryFuture rf = new DefaultQueryFuture(this, true);

    if (pf == null) {
      // query is not in pause, return success directly.
      rf.setSuccess();
      return rf;
    }
    // TODO do the resume stuff
    return rf;
  }

  /**
   * Kill the worker query partition.
   * 
   * */
  @Override
  public final void kill() {
    for (QuerySubTreeTask task : tasks) {
      task.kill();
    }
  }

  @Override
  public final boolean isPaused() {
    return pauseFuture.get() != null;
  }

  @Override
  public final QueryExecutionStatistics getExecutionStatistics() {
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
   * when a REMOVE_WORKER message is received, give tasks another chance to decide if they are ready to generate
   * EOS/EOI.
   */
  public void triggerTasks() {
    for (QuerySubTreeTask task : tasks) {
      task.notifyNewInput();
    }
  }

  /**
   * enable/disable output channels of the root(producer) of each task.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   * */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    for (QuerySubTreeTask task : tasks) {
      task.updateProducerChannels(workerId, enable);
    }
  }

  /**
   * add a recovery task for the failed worker.
   * 
   * @param workerId the id of the failed worker.
   */
  public void addRecoveryTasks(final int workerId) {
    List<RootOperator> recoveryTasks = new ArrayList<RootOperator>();
    for (QuerySubTreeTask task : tasks) {
      if (task.getRootOp() instanceof Producer) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("adding recovery task for " + task.getRootOp().getOpName());
        }
        List<List<TupleBatch>> buffers = ((Producer) task.getRootOp()).getBackupBuffers();
        List<Integer> indices = ((Producer) task.getRootOp()).getChannelIndicesOfAWorker(workerId);
        StreamOutputChannel<TupleBatch>[] channels = ((Producer) task.getRootOp()).getChannels();
        for (int i = 0; i < indices.size(); ++i) {
          int j = indices.get(i);
          /* buffers.get(j) might be an empty List<TupleBatch>, so need to set its schema explicitly. */
          TupleSource scan = new TupleSource(buffers.get(j), task.getRootOp().getSchema());
          scan.setOpName("tuplesource for " + task.getRootOp().getOpName() + channels[j].getID());
          RecoverProducer rp =
              new RecoverProducer(scan, ExchangePairID.fromExisting(channels[j].getID().getStreamID()), channels[j]
                  .getID().getRemoteID(), (Producer) task.getRootOp(), j);
          rp.setOpName("recProducer for " + task.getRootOp().getOpName() + channels[j].getID());
          recoveryTasks.add(rp);
        }
      }
    }
    final List<QuerySubTreeTask> list = new ArrayList<QuerySubTreeTask>();
    for (RootOperator cp : recoveryTasks) {
      QuerySubTreeTask recoveryTask = createTask(cp);
      list.add(recoveryTask);
    }
    new Thread() {
      @Override
      public void run() {
        while (true) {
          if (ownerWorker.getIPCConnectionPool().isRemoteAlive(workerId)) {
            /* waiting for ADD_WORKER to be received */
            for (QuerySubTreeTask task : list) {
              init(task);
              /* input might be null but we still need it to run */
              task.notifyNewInput();
            }
            break;
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }.start();
  }
}
