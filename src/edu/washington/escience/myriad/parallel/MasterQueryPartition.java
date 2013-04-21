package edu.washington.escience.myriad.parallel;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;
import edu.washington.escience.myriad.util.DateTimeUtils;

/**
 * A {@link MasterQueryPartition} is the partition of a query plan at the Master side. Currently, a master query
 * partition can only have a single task.
 * 
 * 
 * */
public class MasterQueryPartition implements QueryPartition {

  /**
   * Record worker execution info.
   * */
  private class WorkerExecutionInfo {
    /**
     * @param workerID owner worker id of the partition.
     * @param workerPlan the query plan of this partition.
     * */
    WorkerExecutionInfo(final int workerID, final RootOperator[] workerPlan) {
      this.workerID = workerID;
      this.workerPlan = workerPlan;
      workerReceiveQuery = new DefaultQueryFuture(MasterQueryPartition.this, false);
      workerReceiveQuery.addListener(new QueryFutureListener() {

        @Override
        public void operationComplete(final QueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowReceived.incrementAndGet();
          workerReceiveFuture.setProgress(1, current, workerExecutionInfo.size());
          if (current >= total) {
            workerReceiveFuture.setSuccess();
          }
        }
      });
      workerCompleteQuery = new DefaultQueryFuture(MasterQueryPartition.this, false);
      workerCompleteQuery.addListener(new QueryFutureListener() {

        @Override
        public void operationComplete(final QueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowCompleted.incrementAndGet();
          queryExecutionFuture.setProgress(1, current, workerExecutionInfo.size());
          if (!future.isSuccess()) {
            failedQueryPartitions.put(workerID, future.getCause());
          }
          if (current >= total) {
            queryStatistics.markQueryEnd();
            if (LOGGER.isInfoEnabled()) {
              LOGGER.info("Query #" + queryID + " executed for "
                  + DateTimeUtils.nanoElapseToHumanReadable(queryStatistics.getQueryExecutionElapse()));
            }

            if (failedQueryPartitions.isEmpty()) {
              queryExecutionFuture.setSuccess();
            } else {
              StringBuilder errorMessage = new StringBuilder();
              int numStackTraceElements = 0;
              for (Entry<Integer, Throwable> workerIDCause : failedQueryPartitions.entrySet()) {
                numStackTraceElements += workerIDCause.getValue().getStackTrace().length;
              }
              DbException currentStackTrace =
                  new DbException("Query #" + future.getQuery().getQueryID() + " failed.\n");
              StackTraceElement[] newStackTrace =
                  new StackTraceElement[currentStackTrace.getStackTrace().length + numStackTraceElements];
              System.arraycopy(currentStackTrace.getStackTrace(), 0, newStackTrace, 0, currentStackTrace
                  .getStackTrace().length);
              int stackTraceShift = currentStackTrace.getStackTrace().length;
              errorMessage.append(currentStackTrace.getMessage());
              for (Entry<Integer, Throwable> workerIDCause : failedQueryPartitions.entrySet()) {
                int failedWorkerID = workerIDCause.getKey();
                Throwable cause = workerIDCause.getValue();
                StackTraceElement[] e = cause.getStackTrace();
                System.arraycopy(e, 0, newStackTrace, stackTraceShift, e.length);
                stackTraceShift += e.length;
                errorMessage.append("\t" + cause.getClass().getName());
                errorMessage.append(" in worker #");
                errorMessage.append(failedWorkerID);
                errorMessage.append(", with message: [");
                errorMessage.append(cause.getMessage());
                errorMessage.append("].\n");
              }
              currentStackTrace.setStackTrace(newStackTrace);
              DbException composedException = new DbException(errorMessage.toString());
              composedException.setStackTrace(newStackTrace);
              queryExecutionFuture.setFailure(composedException);
            }
          }
        }
      });
    }

    final int workerID;
    final RootOperator[] workerPlan;
    final QueryFuture workerReceiveQuery;
    final QueryFuture workerCompleteQuery;
  }

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
   * The owner master.
   * */
  private final Server master;

  /**
   * The priority.
   * */
  private volatile int priority;

  private final ConcurrentHashMap<Integer, WorkerExecutionInfo> workerExecutionInfo;

  private final AtomicInteger nowReceived = new AtomicInteger();
  private final AtomicInteger nowCompleted = new AtomicInteger();

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
   * record all failed query partitions.
   * */
  private final ConcurrentHashMap<Integer, Throwable> failedQueryPartitions =
      new ConcurrentHashMap<Integer, Throwable>();

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
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setSuccess();
      } else {
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setFailure(future.getCause());
      }

    }

  };

  /**
   * Callback when a query plan is received by a worker.
   * 
   * @param workerID the workerID
   * */
  final void queryReceivedByWorker(final int workerID) {
    WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    wei.workerReceiveQuery.setSuccess();
  }

  /**
   * @return worker plans.
   * */
  final Map<Integer, RootOperator[]> getWorkerPlans() {
    Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>(workerExecutionInfo.size());
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() != MyriaConstants.MASTER_ID) {
        result.put(e.getKey(), e.getValue().workerPlan);
      }
    }
    return result;
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
    return workerExecutionInfo.keySet();
  }

  /**
   * Callback when a worker completes its part of the query.
   * 
   * @param workerID the workerID
   * */
  final void workerComplete(final int workerID) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (succeed) message from worker " + workerID + " who is not assigned to query"
          + queryID);
      return;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received query complete (succeed) message from worker: {}", workerID);
    }

    wei.workerCompleteQuery.setSuccess();
  }

  /**
   * Callback when a worker fails in executing its part of the query.
   * 
   * @param workerID the workerID
   * @param cause the cause of the failure
   * */
  final void workerFail(final int workerID, final Throwable cause) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (fail) message from worker " + workerID + " who is not assigned to query"
          + queryID);
      return;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received query complete (fail) message from worker: {}", workerID);
    }
    wei.workerCompleteQuery.setFailure(cause);
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
    workerExecutionInfo = new ConcurrentHashMap<Integer, WorkerExecutionInfo>(workerPlans.size());

    for (Entry<Integer, RootOperator[]> workerInfo : workerPlans.entrySet()) {
      workerExecutionInfo.put(workerInfo.getKey(), new WorkerExecutionInfo(workerInfo.getKey(), workerInfo.getValue()));
    }
    WorkerExecutionInfo masterPart = new WorkerExecutionInfo(MyriaConstants.MASTER_ID, new RootOperator[] { rootOp });
    masterPart.workerReceiveQuery.setSuccess();
    workerExecutionInfo.put(MyriaConstants.MASTER_ID, masterPart);

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
    rootTask.kill();
    master.sendKillMessage(this).awaitUninterruptibly();
  }

  @Override
  public final boolean isPaused() {
    return pauseFuture.get() != null;
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
