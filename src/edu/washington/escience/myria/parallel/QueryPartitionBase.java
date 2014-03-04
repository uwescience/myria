package edu.washington.escience.myria.parallel;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;

/**
 * Implemented basic life cycle management of query partitions and other common stuff.
 * */
public abstract class QueryPartitionBase implements QueryPartition {

  /**
   * Just started.
   * */
  public static final int STATE_START = 0x00;
  /**
   * Initialized.
   * */
  public static final int STATE_INITIALIZED = 0x01;
  /**
   * Paused.
   * */
  public static final int STATE_PAUSED = 0x02;
  /**
   * Running.
   * */
  public static final int STATE_RUNNING = 0x03;
  /**
   * Killed.
   * */
  public static final int STATE_KILLED = 0x04;
  /**
   * Completed.
   * */
  public static final int STATE_COMPLETED = 0x05;

  /**
   * Query state.
   * */
  private int state = STATE_START;

  /**
   * Query state lock.
   * */
  private final Object stateLock = new Object();

  /**
   * @param state the state
   * @return name of the state
   * */
  public static String stateToString(final int state) {
    switch (state) {
      case STATE_START:
        return "START";
      case STATE_INITIALIZED:
        return "INITIALIZED";
      case STATE_RUNNING:
        return "RUNNING";
      case STATE_PAUSED:
        return "PAUSED";
      case STATE_KILLED:
        return "KILLED";
      case STATE_COMPLETED:
        return "COMPLETED";
      default:
        return null;
    }
  }

  /**
   * The future object denoting the query execution progress.
   * */
  private final DefaultQueryFuture executionFuture = new DefaultQueryFuture(this, false);

  /**
   * logger for profile.
   */
  protected static final org.slf4j.Logger PROFILING_LOGGER = org.slf4j.LoggerFactory.getLogger("profile");

  /**
   * The query ID.
   * */
  private final long queryID;

  /**
   * All tasks in the query, not including recovery tasks.
   * */
  private final Set<QuerySubTreeTask> tasks;

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
   * record all failed tasks.
   * */
  private final ConcurrentLinkedQueue<QuerySubTreeTask> failTasks = new ConcurrentLinkedQueue<QuerySubTreeTask>();

  /**
   * Current alive worker set.
   * */
  private final Set<Integer> missingWorkers;

  /***
   * Statistics of this query partition.
   */
  private final QueryExecutionStatistics queryStatistics = new QueryExecutionStatistics();

  /**
   * IPC pool.
   * */
  private final IPCConnectionPool ipcPool;

  /**
   * Root operators.
   * */
  private final List<RootOperator> rootOps;

  /**
   * @param plan the plan of this query partition.
   * @param queryID the id of the query.
   * @param ipcPool IPC pool.
   * */
  public QueryPartitionBase(final SingleQueryPlanWithArgs plan, final long queryID, final IPCConnectionPool ipcPool) {
    this.queryID = queryID;
    ftMode = plan.getFTMode();
    profilingMode = plan.isProfilingMode();
    rootOps = plan.getRootOps();
    this.ipcPool = ipcPool;
    tasks = new HashSet<QuerySubTreeTask>(rootOps.size());
    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
  }

  /**
   * Create initial query tasks.
   * */
  protected void createInitialTasks() {
    if (tasks.size() <= 0) {
      for (final RootOperator taskRootOp : rootOps) {
        tasks.add(createTask(taskRootOp));
      }
    }
  }

  /**
   * @return the executor for running the operator tree rooted by the root
   * @param root the RootOperator of the task.
   * */
  protected abstract ExecutorService getTaskExecutor(final RootOperator root);

  /**
   * @param root the RootOperator of the task to which the Consumer belongs
   * @param c the consumer operator
   * @return the {@link StreamInputBuffer} that the data for the consumer should be put into.
   * */
  protected abstract StreamInputBuffer<TupleBatch> getInputBuffer(final RootOperator root, final Consumer c);

  /**
   * create a task.
   * 
   * @param root the root operator of this task.
   * @return the task.
   */
  protected final QuerySubTreeTask createTask(final RootOperator root) {
    final QuerySubTreeTask drivingTask = new QuerySubTreeTask(ipcPool.getMyIPCID(), this, root, getTaskExecutor(root));

    HashSet<Consumer> consumerSet = new HashSet<Consumer>();
    consumerSet.addAll(drivingTask.getInputChannels().values());
    for (final Consumer c : consumerSet) {
      StreamInputBuffer<TupleBatch> inputBuffer = getInputBuffer(root, c);
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

  @Override
  public final QueryFuture init() {
    DefaultQueryFuture future = null;
    synchronized (stateLock) {
      switch (state) {
        case STATE_KILLED:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Query #" + getQueryID()
              + "already gets killed"));
        case STATE_COMPLETED:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Query #" + getQueryID()
              + "already completed."));
        case STATE_INITIALIZED:
        case STATE_PAUSED:
        case STATE_RUNNING:
          return DefaultQueryFuture.succeededFuture(this);
        case STATE_START:
          state = STATE_INITIALIZED;
          future = new DefaultQueryFuture(this, false);
          break;
        default:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Unknown query state: " + state));
      }
    }
    init(future);
    return future;
  }

  /**
   * Do the actual initialization.
   * 
   * @param future the future object representing the init action.
   * */
  protected abstract void init(final DefaultQueryFuture future);

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
  public String toString() {
    return tasks + ", priority:" + priority;
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  @Override
  public DefaultQueryFuture getExecutionFuture() {
    return executionFuture;
  }

  /**
   * @param executionFuture the future object representing the execution of the whole query partition.
   * */
  protected abstract void startExecution(final QueryFuture executionFuture);

  @Override
  public QueryFuture startExecution() {
    synchronized (stateLock) {
      switch (state) {
        case STATE_START:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Query #" + getQueryID()
              + " not yet initialized"));
        case STATE_KILLED:
        case STATE_COMPLETED:
        case STATE_RUNNING:
        case STATE_PAUSED:
          return getExecutionFuture();
        case STATE_INITIALIZED:
          state = STATE_RUNNING;
          getExecutionFuture().addListener(new QueryFutureListener() {

            @Override
            public void operationComplete(final QueryFuture future) throws Exception {
              synchronized (stateLock) {
                if (state != STATE_KILLED) {
                  state = STATE_COMPLETED;
                }
              }
            }
          });
          break;
        default:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Query #" + getQueryID()
              + " Unknown query state: " + state));
      }
    }
    startExecution(executionFuture);
    return executionFuture;
  }

  @Override
  public final QueryFuture kill() {
    DefaultQueryFuture killFuture;
    synchronized (stateLock) {
      switch (state) {
        case STATE_START:
          state = STATE_KILLED;
          getExecutionFuture().setFailure(new QueryKilledException());
        case STATE_KILLED:
        case STATE_COMPLETED:
          return DefaultQueryFuture.succeededFuture(this);
        case STATE_INITIALIZED:
        case STATE_PAUSED:
        case STATE_RUNNING:
          state = STATE_KILLED;
          final DefaultQueryFuture killFuture0 = new DefaultQueryFuture(this, false);
          killFuture = killFuture0;
          getExecutionFuture().addListener(new QueryFutureListener() {

            @Override
            public void operationComplete(final QueryFuture future) throws Exception {
              killFuture0.setSuccess();
            }
          });
          break;
        default:
          return DefaultQueryFuture.failedFuture(this, new IllegalStateException("Query #" + getQueryID()
              + " Unknown query state: " + state));
      }
    }
    kill(killFuture);
    return killFuture;
  }

  /**
   * @param future the future object representing the kill action.
   * */
  protected abstract void kill(final DefaultQueryFuture future);

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

  /**
   * @return IPC Pool.
   * */
  protected final IPCConnectionPool getIPCPool() {
    return ipcPool;
  }

  @Override
  public final Set<Integer> getMissingWorkers() {
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
   * @return the query tasks.
   * */
  protected final Set<QuerySubTreeTask> getTasks() {
    return tasks;
  }

  /**
   * @return the failed query tasks.
   * */
  protected final ConcurrentLinkedQueue<QuerySubTreeTask> getFailTasks() {
    return failTasks;
  }

  /**
   * @return If the query has been asked to get killed (the kill event may not have completed).
   * */
  public final boolean isKilled() {
    synchronized (stateLock) {
      return state == STATE_KILLED;
    }
  }

}
