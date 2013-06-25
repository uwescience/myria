package edu.washington.escience.myriad.parallel;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;
import edu.washington.escience.myriad.util.DateTimeUtils;

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
  private final QueryFuture executionFuture = new DefaultQueryFuture(this, true);

  /**
   * record all failed tasks.
   * */
  private final ConcurrentLinkedQueue<QuerySubTreeTask> failTasks = new ConcurrentLinkedQueue<QuerySubTreeTask>();

  /**
   * The future listener for processing the complete events of the execution of all the query's tasks.
   * */
  private final QueryFutureListener taskExecutionListener = new QueryFutureListener() {

    @Override
    public void operationComplete(final QueryFuture future) throws Exception {
      QuerySubTreeTask drivingTask = (QuerySubTreeTask) (future.getAttachment());
      int currentNumFinished = numFinishedTasks.incrementAndGet();

      executionFuture.setProgress(1, currentNumFinished, tasks.size());
      Throwable failureReason = future.getCause();
      if (!future.isSuccess()) {
        failTasks.add(drivingTask);
        if (!(failureReason instanceof QueryKilledException)) {
          // The task is a failure, not killed.
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
          // TODO currently use the cause of the first failed task as the case of the failure of the whole query.
          executionFuture.setFailure(failTasks.peek().getExecutionFuture().getCause());
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
   * @param operators the operators belonging to this query partition.
   * @param queryID the id of the query.
   * @param ownerWorker the worker on which this query partition is going to run
   * */
  public WorkerQueryPartition(final RootOperator[] operators, final long queryID, final Worker ownerWorker) {
    this.queryID = queryID;
    tasks = new HashSet<QuerySubTreeTask>(operators.length);
    numFinishedTasks = new AtomicInteger(0);
    this.ownerWorker = ownerWorker;
    producerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ProducerChannel>();
    consumerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ConsumerChannel>();
    for (final RootOperator taskRootOp : operators) {
      final QuerySubTreeTask drivingTask =
          new QuerySubTreeTask(ownerWorker.getIPCConnectionPool().getMyIPCID(), this, taskRootOp, ownerWorker
              .getQueryExecutor());
      QueryFuture taskExecutionFuture = drivingTask.getExecutionFuture();
      taskExecutionFuture.addListener(taskExecutionListener);

      tasks.add(drivingTask);

      for (Consumer c : drivingTask.getInputChannels().values()) {
        for (ConsumerChannel cc : c.getExchangeChannels()) {
          consumerChannelMapping.putIfAbsent(cc.getExchangeChannelID(), cc);
        }
      }

      for (ExchangeChannelID producerID : drivingTask.getOutputChannels()) {
        producerChannelMapping.put(producerID, new ProducerChannel(drivingTask, producerID));
      }

    }

    final FlowControlHandler fch = ownerWorker.getFlowControlHandler();
    for (ConsumerChannel cChannel : consumerChannelMapping.values()) {
      // setup input buffers.
      final Consumer operator = cChannel.getOwnerConsumer();

      FlowControlInputBuffer<ExchangeData> inputBuffer =
          new FlowControlInputBuffer<ExchangeData>(ownerWorker.getInputBufferCapacity());
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
    }
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
      t.init(ImmutableMap.copyOf(ownerWorker.getExecEnvVars()));
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
    QueryFuture rf = new DefaultQueryFuture(this, true);

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

}
