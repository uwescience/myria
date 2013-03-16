package edu.washington.escience.myriad.parallel;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.commons.io.FilenameUtils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;
import edu.washington.escience.myriad.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.JVMUtils;

/**
 * Workers do the real query execution. A query received by the server will be pre-processed and then dispatched to the
 * workers.
 * 
 * To execute a query on a worker, 4 steps are proceeded:
 * 
 * 1) A worker receive an Operator instance as its execution plan. The worker then stores the plan and does some
 * pre-processing, e.g. initializes the data structures which are needed during the execution of the plan.
 * 
 * 2) Each worker sends back to the server a message (it's id) to notify the server that the query plan has been
 * successfully received. And then each worker waits for the server to send the "start" message.
 * 
 * 3) Each worker executes its query plan after "start" is received.
 * 
 * 4) After the query plan finishes, each worker removes the query plan and related data structures, and then waits for
 * next query plan
 * 
 */
public final class Worker {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Worker.class.getName());

  /**
   * query execution mode, blocking or non-blocking. Always use the NON_BLOCKING mode. The BLOCKING mode may not work
   * and may get abandoned.
   * */
  @Deprecated
  static enum QueryExecutionMode {
    /**
     * blocking execution, call next() and fetchNext().
     * */
    BLOCKING,

    /**
     * non-blocking execution, call nextReady() and fetchNextReady().
     * */
    NON_BLOCKING;
  }

  /**
   * Control message processor.
   * */
  private final class ControlMessageProcessor implements Runnable {
    @Override
    public void run() {
      try {

        TERMINATE_MESSAGE_PROCESSING : while (true) {
          if (Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
            break TERMINATE_MESSAGE_PROCESSING;
          }

          ControlMessage cm = null;
          try {
            while ((cm = controlMessageQueue.take()) != null) {
              switch (cm.getType()) {
                case SHUTDOWN:
                  if (LOGGER.isInfoEnabled()) {
                    if (LOGGER.isInfoEnabled()) {
                      LOGGER.info("shutdown requested");
                    }
                  }
                  toShutdown = true;
                  abruptShutdown = false;
                  break;
                case START_QUERY:
                  long queryId = cm.getQueryId();
                  WorkerQueryPartition q = activeQueries.get(queryId);
                  if (q == null) {
                    if (LOGGER.isErrorEnabled()) {
                      LOGGER.error("Unknown query id: {}, current active queries are: {}", queryId, activeQueries);
                    }
                    continue;
                  }
                  if (queryExecutionMode == QueryExecutionMode.NON_BLOCKING) {
                    q.startNonBlockingExecution();
                  } else {
                    q.startBlockingExecution();
                  }
                  break;
              }
            }
          } catch (InterruptedException e) {
            Thread.interrupted();
            break;
          }
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at control message processing.", ee);
        }
      }
    }
  }

  /**
   * The non-blocking query driver. It calls root.nextReady() to start a qurey.
   */
  private class QueryMessageProcessor implements Runnable {

    @Override
    public final void run() {
      try {
        WorkerQueryPartition q = null;
        while (true) {
          try {
            q = queryQueue.take();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }

          if (q != null) {
            try {
              receiveQuery(q);
              sendMessageToMaster(IPCUtils.queryReadyTM(q.getQueryID()));
            } catch (DbException e) {
              if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Unexpected exception at preparing query. Drop the query.", e);
              }
              q = null;
            }
          }
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at query nonblocking driver.", ee);
        }
      }
    }

  }

  /**
   * The controller class which decides whether this worker should shutdown or not. 1) it detects whether the server is
   * still alive. If the server got killed because of any reason, the workers will be terminated. 2) it detects whether
   * a shutdown message is received.
   */
  private class Reporter extends TimerTask {

    @Override
    public final synchronized void run() {
      Channel serverChannel = null;
      try {
        serverChannel = connectionPool.reserveLongTermConnection(MyriaConstants.MASTER_ID);
        if (IPCUtils.isRemoteConnected(serverChannel)) {
          return;
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at reporter.", ee);
        }
      } finally {
        if (serverChannel != null) {
          connectionPool.releaseLongTermConnection(serverChannel);
        }
      }
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("The Master has shutdown, I'll shutdown now.");
      }
      toShutdown = true;
      abruptShutdown = true;
      cancel();
    }
  }

  /**
   * Periodically detect whether the {@link Worker} should be shutdown.
   * */
  private class ShutdownChecker extends TimerTask {
    @Override
    public final synchronized void run() {
      try {
        if (toShutdown) {
          shutdown();
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at shutdown checker.", ee);
        }
      }

    }
  }

  static final String usage = "Usage: worker [--conf <conf_dir>]";

  /**
   * {@link ExecutorService} for query executions.
   * */
  private volatile ThreadPoolExecutor queryExecutor;

  /**
   * {@link ExecutorService} for non-query message processing.
   * */
  private volatile ExecutorService messageProcessingExecutor;

  /**
   * current active queries. queryID -> QueryPartition
   * */
  private final ConcurrentHashMap<Long, WorkerQueryPartition> activeQueries;

  /**
   * My message handler.
   * */
  final WorkerDataHandler workerDataHandler;

  /**
   * IPC flow controller.
   * */
  final FlowControlHandler flowController;

  /**
   * timer task executor.
   * */
  private ScheduledExecutorService scheduledTaskExecutor;

  private final Properties databaseHandle;

  /**
   * The ID of this worker.
   */
  final int myID;

  /**
   * connectionPool[0] is always the master.
   */
  final IPCConnectionPool connectionPool;

  /**
   * A indicator of shutting down the worker.
   */
  private volatile boolean toShutdown = false;

  /**
   * abrupt shutdown.
   * */
  private volatile boolean abruptShutdown = false;

  /**
   * Message queue for control messages.
   * */
  final LinkedBlockingQueue<ControlMessage> controlMessageQueue;

  /**
   * Message queue for queries.
   * */
  final PriorityBlockingQueue<WorkerQueryPartition> queryQueue;

  final WorkerCatalog catalog;

  final SocketInfo masterSocketInfo;

  final SocketInfo mySocketInfo;

  /**
   * Query execution mode. May remove
   * */
  @Deprecated
  final QueryExecutionMode queryExecutionMode;

  /**
   * Producer channel mapping of current active queries.
   * */
  final ConcurrentHashMap<ExchangeChannelID, ProducerChannel> producerChannelMapping;

  /**
   * Consumer channel mapping of current active queries.
   * */
  final ConcurrentHashMap<ExchangeChannelID, ConsumerChannel> consumerChannelMapping;

  /**
   * {@link ExecutorService} for Netty pipelines.
   * */
  volatile OrderedMemoryAwareThreadPoolExecutor pipelineExecutor;

  /**
   * The default input buffer capacity for each {@link Consumer} input buffer.
   * */
  final int inputBufferCapacity;

  public final String workingDirectory;

  /**
   * Execution environment variables for operators.
   * */
  final ConcurrentHashMap<String, Object> execEnvVars;

  public static void main(String[] args) {
    try {
      java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
      java.util.logging.Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

      if (args.length > 2) {
        LOGGER.warn("Invalid number of arguments.\n" + usage);
        JVMUtils.shutdownVM();
      }

      String workingDir = null;
      if (args.length >= 2) {
        if (args[0].equals("--workingDir")) {
          workingDir = args[1];
        } else {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Invalid arguments.\n" + usage);
          }
          JVMUtils.shutdownVM();
        }
      }

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("workingDir: " + workingDir);
      }
      // Instantiate a new worker
      final Worker w = new Worker(workingDir, QueryExecutionMode.NON_BLOCKING);
      // int port = w.port;

      // Start the actual message handler by binding
      // the acceptor to a network socket
      // Now the worker can accept messages
      w.start();

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Worker started at:" + w.catalog.getWorkers().get(w.myID));
      }
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unknown error occurs at Worker. Quit directly.", e);
      }
    }

  }

  public Worker(final String workingDirectory, final QueryExecutionMode mode) throws CatalogException,
      FileNotFoundException {
    queryExecutionMode = mode;
    catalog = WorkerCatalog.open(FilenameUtils.concat(workingDirectory, "worker.catalog"));

    this.workingDirectory = workingDirectory;
    myID = Integer.parseInt(catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_IDENTIFIER));
    databaseHandle = new Properties();

    mySocketInfo = catalog.getWorkers().get(myID);

    controlMessageQueue = new LinkedBlockingQueue<ControlMessage>();
    queryQueue = new PriorityBlockingQueue<WorkerQueryPartition>();

    masterSocketInfo = catalog.getMasters().get(0);
    workerDataHandler = new WorkerDataHandler(this);

    final Map<Integer, SocketInfo> workers = catalog.getWorkers();
    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(MyriaConstants.MASTER_ID, masterSocketInfo);

    connectionPool =
        new IPCConnectionPool(myID, computingUnits, IPCConfigurations.createWorkerIPCServerBootstrap(this),
            IPCConfigurations.createWorkerIPCClientBootstrap(this));
    activeQueries = new ConcurrentHashMap<Long, WorkerQueryPartition>();
    producerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ProducerChannel>();
    consumerChannelMapping = new ConcurrentHashMap<ExchangeChannelID, ConsumerChannel>();
    flowController = new FlowControlHandler(consumerChannelMapping, producerChannelMapping);

    inputBufferCapacity =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));
    execEnvVars = new ConcurrentHashMap<String, Object>();

    for (Entry<String, String> cE : catalog.getAllConfigurations().entrySet()) {
      execEnvVars.put(cE.getKey(), cE.getValue());
    }
    final String databaseType = catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_STORAGE_SYSTEM_TYPE);
    switch (databaseType) {
      case "sqlite":
        String sqliteFilePath = catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_DATA_SQLITE_DB);
        databaseHandle.setProperty("sqliteFile", sqliteFilePath);
        execEnvVars.put("sqliteFile", sqliteFilePath);
        break;
      case "mysql":
        /* TODO fill this in. */
        break;
      default:
        throw new CatalogException("Unknown worker type: " + databaseType);
    }

    execEnvVars.put("ipcConnectionPool", connectionPool);
  }

  /**
   * This method should be called when a query is finished.
   * 
   * @param query the query that just finished.
   */
  public void finishQuery(final WorkerQueryPartition query) {
    if (query != null) {
      activeQueries.remove(query.getQueryID());
      sendMessageToMaster(IPCUtils.queryCompleteTM(query.getQueryID())).addListener(new ChannelFutureListener() {

        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("The query complete message is sent to the master for sure ");
            }
          }
        }

      });
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("My part of query " + query + " finished");
      }
    }
  }

  /**
   * this method should be called when a query is received from the server.
   * 
   * It does the initialization and preparation for the execution of the query.
   * 
   * @param query the received query.
   * @throws DbException if any error occurs.
   */
  public void receiveQuery(final WorkerQueryPartition query) throws DbException {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query received" + query);
    }
    setupExchangeChannels(query);
    activeQueries.put(query.getQueryID(), query);
  }

  /**
   * Find out the consumers and producers and register them in the {@link Worker}'s data structures
   * {@link Worker#producerChannelMapping} and {@link Worker#consumerChannelMapping}.
   * 
   */
  public void setupExchangeChannels(final WorkerQueryPartition query) throws DbException {
    final ArrayList<QuerySubTreeTask> tasks = new ArrayList<QuerySubTreeTask>();

    for (final RootOperator task : query.getOperators()) {
      QuerySubTreeTask drivingTask = new QuerySubTreeTask(myID, query, task, queryExecutor, queryExecutionMode);
      tasks.add(drivingTask);

      if (task instanceof Producer) {
        // Setup producer channels.
        Producer p = (Producer) task;
        ExchangePairID[] oIDs = p.operatorIDs();
        int[] destWorkers = p.getDestinationWorkerIDs(myID);
        for (int i = 0; i < destWorkers.length; i++) {
          ExchangeChannelID ecID = new ExchangeChannelID(oIDs[i].getLong(), destWorkers[i]);
          producerChannelMapping.put(ecID, new ProducerChannel(drivingTask, p, ecID));
        }
      }
      setupConsumerChannels(task, drivingTask);
    }

    query.setTasks(tasks);

  }

  public void setupConsumerChannels(final Operator currentOperator, final QuerySubTreeTask drivingTask)
      throws DbException {

    if (currentOperator == null) {
      return;
    }

    if (currentOperator instanceof Consumer) {
      final Consumer operator = (Consumer) currentOperator;
      FlowControlInputBuffer<ExchangeData> inputBuffer =
          new FlowControlInputBuffer<ExchangeData>(inputBufferCapacity, operator.getOperatorID());
      inputBuffer.addBufferFullListener(new IPCEventListener<FlowControlInputBuffer<ExchangeData>>() {
        @Override
        public void triggered(final IPCEvent<FlowControlInputBuffer<ExchangeData>> e) {
          if (e.getAttachment().remainingCapacity() <= 0) {
            flowController.pauseRead(operator).awaitUninterruptibly();
          }
        }
      });
      inputBuffer.addBufferRecoverListener(new IPCEventListener<FlowControlInputBuffer<ExchangeData>>() {
        @Override
        public void triggered(final IPCEvent<FlowControlInputBuffer<ExchangeData>> e) {
          if (e.getAttachment().remainingCapacity() > 0) {
            flowController.resumeRead(operator).awaitUninterruptibly();
          }
        }
      });
      operator.setInputBuffer(inputBuffer);

      operator.exchangeChannels = new ConsumerChannel[operator.getSourceWorkers(myID).length];
      ExchangePairID oID = operator.getOperatorID();
      int[] sourceWorkers = operator.getSourceWorkers(myID);
      int idx = 0;
      for (int workerID : sourceWorkers) {
        ExchangeChannelID ecID = new ExchangeChannelID(oID.getLong(), workerID);
        ConsumerChannel cc = new ConsumerChannel(drivingTask, operator, ecID);
        consumerChannelMapping.put(ecID, cc);
        operator.exchangeChannels[idx++] = cc;
      }
    }

    final Operator[] children = currentOperator.getChildren();

    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          setupConsumerChannels(child, drivingTask);
        }
      }
    }
  }

  /**
   * @param message the message to get sent to the master
   * @return the future of this sending action.
   * */
  ChannelFuture sendMessageToMaster(final TransportMessage message) {
    return Worker.this.connectionPool.sendShortMessage(MyriaConstants.MASTER_ID, message);
  }

  /**
   * This method should be called whenever the system is going to shutdown.
   * 
   * @throws InterruptedException if the shutdown process is interrupted.
   */
  public void shutdown() throws InterruptedException {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Shutdown requested. Please wait when cleaning up...");
    }
    if (!connectionPool.isShutdown()) {
      if (!abruptShutdown) {
        connectionPool.shutdown().await();
      } else {
        connectionPool.shutdownNow().await();
      }
    }
    connectionPool.releaseExternalResources();

    if (pipelineExecutor != null && !pipelineExecutor.isShutdown()) {
      pipelineExecutor.shutdown();
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("shutdown IPC completed");
    }
    // must use shutdownNow here because the query queue processor and the control message processor are both blocking.
    // We have to interrupt them at shutdown.
    messageProcessingExecutor.shutdownNow();
    queryExecutor.shutdown();
    scheduledTaskExecutor.shutdown();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Worker #" + myID + " shutdown completed");
    }
  }

  /**
   * Start the worker service.
   * 
   * @throws Exception if any error meets.
   * */
  public void start() throws Exception {
    ExecutorService bossExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("IPC boss"));
    ExecutorService workerExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("IPC worker"));
    pipelineExecutor =
        new OrderedMemoryAwareThreadPoolExecutor(3, 0, 0, 600, TimeUnit.SECONDS, new RenamingThreadFactory(
            "Pipeline executor"));

    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(bossExecutor, workerExecutor,
            Runtime.getRuntime().availableProcessors() * 2 + 1);

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(bossExecutor, workerExecutor,
            Runtime.getRuntime().availableProcessors() * 2 + 1);

    ChannelPipelineFactory serverPipelineFactory = new IPCPipelineFactories.WorkerServerPipelineFactory(this);
    ChannelPipelineFactory clientPipelineFactory = new IPCPipelineFactories.WorkerClientPipelineFactory(this);
    ChannelPipelineFactory workerInJVMPipelineFactory = new IPCPipelineFactories.WorkerInJVMPipelineFactory(this);

    connectionPool.start(serverChannelFactory, serverPipelineFactory, clientChannelFactory, clientPipelineFactory,
        workerInJVMPipelineFactory, new InJVMLoopbackChannelSink<TransportMessage>(workerDataHandler, myID));

    if (queryExecutionMode == QueryExecutionMode.NON_BLOCKING) {
      int numCPU = Runtime.getRuntime().availableProcessors();
      queryExecutor =
          new ThreadPoolExecutor(numCPU, numCPU, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
              new RenamingThreadFactory("Nonblocking query executor"));
    } else {// blocking query execution
      queryExecutor =
          (ThreadPoolExecutor) Executors.newCachedThreadPool(new RenamingThreadFactory("Blocking query executor"));
    }
    messageProcessingExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Control message processor"));
    messageProcessingExecutor.submit(new QueryMessageProcessor());
    messageProcessingExecutor.submit(new ControlMessageProcessor());
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(new RenamingThreadFactory("Worker global timer"));
    scheduledTaskExecutor.scheduleAtFixedRate(new ShutdownChecker(), 500, 500, TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(new Reporter(), (long) (Math.random() * 3000) + 5000, (long) (Math
        .random() * 2000) + 1000, TimeUnit.MILLISECONDS);
    /* Tell the master we're alive. */
    sendMessageToMaster(IPCUtils.CONTROL_WORKER_ALIVE).awaitUninterruptibly();
  }

  /**
   * @param configKey config key.
   * @return a worker configuration.
   * */
  public String getConfiguration(final String configKey) {
    try {
      return catalog.getConfigurationValue(configKey);
    } catch (CatalogException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("Configuration retrieval error", e);
      }
      return null;
    }
  }
}
