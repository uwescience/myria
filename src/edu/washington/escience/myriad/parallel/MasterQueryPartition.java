package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;
import edu.washington.escience.myriad.util.DateTimeUtils;
import edu.washington.escience.myriad.util.ReentrantSpinLock;

/**
 * A {@link MasterQueryPartition} is the partition of a query plan at the Master side. Currently, a master query
 * partition can only have a single task.
 * 
 * 
 * */
public class MasterQueryPartition implements QueryPartition {

  /**
   * logger.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterQueryPartition.class);

  /**
   * The query ID.
   * */
  private final long queryID;

  /**
   * The execution task for the master side query partition.
   * */
  private volatile QuerySubTreeTask rootTask;

  private final QueryExecutionStatistics queryStatistics = new QueryExecutionStatistics();

  /**
   * The root operator of the master query partition.
   * */
  private final RootOperator root;

  /**
   * The worker plans of the owner query of this master query partition.
   * */
  private final ConcurrentHashMap<Integer, RootOperator[]> workerPlans;

  /**
   * The owner master.
   * */
  private final Server master;

  /**
   * The priority.
   * */
  private volatile int priority;

  /**
   * The workers who have received their part of the query plan.
   * */
  private final BitSet workersReceivedQuery;

  /**
   * BitSet is not thread safe. Protect {@code workersReceivedQuery}.
   * */
  private final ReentrantSpinLock workersReceivedQueryLock = new ReentrantSpinLock();

  /**
   * The workers who have completed their part of the query plan.
   * */
  private final BitSet workersCompleteQuery;

  /**
   * BitSet is not thread safe. Protect {@code workersCompleteQuery}.
   * */
  private final ReentrantSpinLock workersCompleteQueryLock = new ReentrantSpinLock();

  /**
   * The workers get assigned to compute the query. WorkerID -> Worker Index.
   * */
  private final ConcurrentHashMap<Integer, Integer> workersAssigned;

  /**
   * The future object denoting the worker receive query plan operation.
   * */
  private final QueryFuture workerReceiveFuture = new DefaultQueryFuture(this, false);

  /**
   * The future object denoting the query execution progress.
   * */
  private final QueryFuture queryExecutionFuture = new DefaultQueryFuture(this, false);

  /**
   * Store the current pause future if the query is in pause, otherwise null.
   * */
  private final AtomicReference<QueryFuture> pauseFuture = new AtomicReference<QueryFuture>(null);

  /**
   * is the query killed.
   * */
  private volatile boolean isKilled;

  /**
   * Producer channel mapping of current active queries.
   * */
  private final ConcurrentHashMap<ExchangeChannelID, ProducerChannel> producerChannelMapping;

  /**
   * @return all producer channel mapping in this query partition.
   * */
  final Map<ExchangeChannelID, ProducerChannel> getProducerChannelMapping() {
    return producerChannelMapping;
  }

  /**
   * Consumer channel mapping of current active queries.
   * */
  private final ConcurrentHashMap<ExchangeChannelID, ConsumerChannel> consumerChannelMapping;

  /**
   * @return all consumer channel mapping in this query partition.
   * */
  final Map<ExchangeChannelID, ConsumerChannel> getConsumerChannelMapping() {
    return consumerChannelMapping;
  }

  /**
   * The future listener for processing the complete events of the execution of the master task.
   * */
  private final QueryFutureListener taskExecutionListener = new QueryFutureListener() {

    @Override
    public void operationComplete(final QueryFuture future) throws Exception {
      QuerySubTreeTask drivingTask = (QuerySubTreeTask) (future.getAttachment());
      drivingTask.cleanup();
      if (future.isSuccess()) {
        if (root instanceof SinkRoot) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info(" Root task {} EOS. Num output tuple: {}", rootTask, ((SinkRoot) root).getCount());
          }
        }
      }
      taskOrWorkerComplete();
    }

  };

  /**
   * Callback when a query plan is received by a worker.
   * 
   * @param workerID the workerID
   * */
  final void queryReceivedByWorker(final int workerID) {
    final int workerIdx = workersAssigned.get(workerID);
    int nowCardinality = -1;

    workersReceivedQueryLock.lock();
    try {
      workersReceivedQuery.set(workerIdx);
      nowCardinality = workersReceivedQuery.cardinality();
    } finally {
      workersReceivedQueryLock.unlock();
    }
    workerReceiveFuture.setProgress(1, nowCardinality, workersAssigned.size());
    if (nowCardinality >= workersAssigned.size()) {
      workerReceiveFuture.setSuccess();
    }
  }

  /**
   * @return worker plans.
   * */
  final Map<Integer, RootOperator[]> getWorkerPlans() {
    return workerPlans;
  }

  /**
   * @return query future for the worker receiving query action.
   * */
  final QueryFuture getWorkerReceiveFuture() {
    return workerReceiveFuture;
  }

  /**
   * @return query future for the worker receiving query action.
   * */
  final QueryFuture getQueryExecutionFuture() {
    return queryExecutionFuture;
  }

  /**
   * @return my root operator.
   * */
  final RootOperator getRootOperator() {
    return root;
  }

  /**
   * @return the set of workers get assigned to run the query.
   * */
  final Set<Integer> getWorkerAssigned() {
    return workersAssigned.keySet();
  }

  /**
   * Callback when a worker completes its part of the query.
   * 
   * @param workerID the workerID
   * */
  final void workerComplete(final int workerID) {
    final Integer workerIdx = workersAssigned.get(workerID);
    if (workerIdx == null) {
      LOGGER.warn("Got a QUERY_COMPLETE message from worker " + workerID + " who is not assigned to query" + queryID);
      return;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received query complete message from worker: {}", workerID);
    }
    workersCompleteQueryLock.lock();
    try {
      workersCompleteQuery.set(workerIdx);
    } finally {
      workersCompleteQueryLock.unlock();
    }
    taskOrWorkerComplete();
  }

  /**
   * Call if either any worker completes or the root task completes.
   * */
  private void taskOrWorkerComplete() {
    int nowCardinality = -1;
    workersCompleteQueryLock.lock();
    try {
      nowCardinality = workersCompleteQuery.cardinality();
    } finally {
      workersCompleteQueryLock.unlock();
    }
    queryExecutionFuture.setProgress(1, nowCardinality, workersAssigned.size());
    if (nowCardinality >= workersAssigned.size() && rootTask.getExecutionFuture().isDone()) {
      queryStatistics.markQueryEnd();
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Query #" + queryID + " executed for "
            + DateTimeUtils.nanoElapseToHumanReadable(queryStatistics.getQueryExecutionElapse()));
      }
      // TODO currently only use roottask's success or fail to set the whole query's success or fail.
      // next step: consider the workers' reports also.
      if (rootTask.getExecutionFuture().isSuccess()) {
        queryExecutionFuture.setSuccess();
      } else {
        queryExecutionFuture.setFailure(rootTask.getExecutionFuture().getCause());
      }
    }
  }

  /**
   * @param rootOp the root operator of the master query.
   * @param workerPlans the worker plans.
   * @param queryID queryID.
   * @param master the master on which the query partition is running.
   * */
  public MasterQueryPartition(final RootOperator rootOp, final Map<Integer, RootOperator[]> workerPlans,
      final long queryID, final Server master) {
    root = rootOp;
    this.queryID = queryID;
    this.master = master;
    workersAssigned = new ConcurrentHashMap<Integer, Integer>(workerPlans.size());
    workersReceivedQuery = new BitSet(workerPlans.size());

    // the master part of the query plan always get index of 0
    int idx = 1;
    for (Integer workerID : workerPlans.keySet()) {
      workersAssigned.put(workerID, idx++);
    }
    workersCompleteQuery = new BitSet(workerPlans.size());
    this.workerPlans = new ConcurrentHashMap<Integer, RootOperator[]>(workerPlans);
    producerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ProducerChannel>();
    consumerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ConsumerChannel>();
    rootTask =
        new QuerySubTreeTask(MyriaConstants.MASTER_ID, this, root, master.getQueryExecutor(),
            QueryExecutionMode.NON_BLOCKING);
    rootTask.getExecutionFuture().addListener(taskExecutionListener);
    for (Consumer c : rootTask.getInputChannels().values()) {
      for (ConsumerChannel cc : c.getExchangeChannels()) {
        consumerChannelMapping.putIfAbsent(cc.getExchangeChannelID(), cc);
      }
    }

    for (ExchangeChannelID producerID : rootTask.getOutputChannels()) {
      producerChannelMapping.put(producerID, new ProducerChannel(rootTask, producerID));
    }

    final FlowControlHandler fch = master.getFlowControlHandler();
    for (ConsumerChannel cChannel : consumerChannelMapping.values()) {
      final Consumer operator = cChannel.getOwnerConsumer();

      FlowControlInputBuffer<ExchangeData> inputBuffer =
          new FlowControlInputBuffer<ExchangeData>(master.getInputBufferCapacity());
      inputBuffer.attach(operator.getOperatorID());
      inputBuffer.addBufferFullListener(new IPCEventListener<FlowControlInputBuffer<ExchangeData>>() {
        @Override
        public void triggered(final IPCEvent<FlowControlInputBuffer<ExchangeData>> e) {
          if (e.getAttachment().remainingCapacity() <= 0) {
            fch.pauseRead(operator).awaitUninterruptibly();
          }
        }
      });
      inputBuffer.addBufferRecoverListener(new IPCEventListener<FlowControlInputBuffer<ExchangeData>>() {
        @Override
        public void triggered(final IPCEvent<FlowControlInputBuffer<ExchangeData>> e) {
          if (e.getAttachment().remainingCapacity() > 0) {
            fch.resumeRead(operator).awaitUninterruptibly();
          }
        }
      });
      inputBuffer.addBufferEmptyListener(new IPCEventListener<FlowControlInputBuffer<ExchangeData>>() {
        @Override
        public void triggered(final IPCEvent<FlowControlInputBuffer<ExchangeData>> e) {
          if (e.getAttachment().remainingCapacity() > 0) {
            fch.resumeRead(operator).awaitUninterruptibly();
          }
        }
      });
      operator.setInputBuffer(inputBuffer);
      master.getDataBuffer().put(operator.getOperatorID(), inputBuffer);
    }

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
    return rootTask + ", priority:" + priority;
  }

  @Override
  public final void startNonBlockingExecution() {
    queryStatistics.markQueryStart();
    rootTask.nonBlockingExecute();
  }

  /**
   * start blocking execution.
   */
  @Deprecated
  public final void startBlockingExecution() {
    queryStatistics.markQueryStart();
    rootTask.blockingExecute();
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  /**
   * Pause the master query partition.
   * 
   * @return the future instance of the pause action. The future will be set as done if and only if all the tasks in
   *         this query have stopped execution. During a pause of the query, all call to this method returns the same
   *         future instance. Two pause calls when the query is not paused at either of the calls return two different
   *         instances.
   */
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
   * Resume the master query partition.
   * 
   * @return the future instance of the resume action.
   * */
  @Override
  public final QueryFuture resume() {
    QueryFuture pf = pauseFuture.getAndSet(null);
    QueryFuture rf = new DefaultQueryFuture(this, true);

    if (pf == null) {
      rf.setSuccess();
      return rf;
    }

    // TODO do the resume stuff
    return rf;
  }

  /**
   * Kill the master query partition.
   * 
   * */
  @Override
  public final void kill() {
    // TODO do the actual kill stuff
    isKilled = true;
    rootTask.kill();
    master.sendKillMessage(this).awaitUninterruptibly();
  }

  @Override
  public final boolean isPaused() {
    return pauseFuture.get() != null;
  }

  @Override
  public final boolean isKilled() {
    return isKilled;
  }

  @Override
  public final void init() {
    rootTask.init(ImmutableMap.copyOf(master.getExecEnvVars()));
  }

  @Override
  public final QueryExecutionStatistics getExecutionStatistics() {
    return queryStatistics;
  }

}
