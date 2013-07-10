package edu.washington.escience.myriad.parallel;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myriad.parallel.ipc.ShortMessageProcessor;
import edu.washington.escience.myriad.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myriad.parallel.ipc.StreamOutputChannel;
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
  public static enum QueryExecutionMode {
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
                default:
                  if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Unexpected control message received at worker: " + cm.getType());
                  }
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
  static final class Reporter extends TimerTask {

    /**
     * Delay of the first check. 3 seconds to 5 seconds.
     * */
    public static final int INITIAL_DELAY_IN_MS = (3000 + (int) (2000 * Math.random()));

    /**
     * Time interval between two checks. 2.4 seconds to 2.9 seconds.
     * */
    public static final int INTERVAL = (2400 + (int) (500 * Math.random()));

    /**
     * the owner worker.
     * */
    private final Worker owner;

    /**
     * @param owner the owner worker.
     * */
    Reporter(final Worker owner) {
      this.owner = owner;
    }

    @Override
    public synchronized void run() {
      StreamOutputChannel<TransportMessage> serverChannel = null;
      try {
        serverChannel = owner.connectionPool.<TransportMessage> reserveLongTermConnection(MyriaConstants.MASTER_ID, 0);
        if (IPCUtils.isRemoteConnected(serverChannel)) {
          return;
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at reporter.", ee);
        }
      } finally {
        if (serverChannel != null) {
          owner.connectionPool.releaseLongTermConnection(serverChannel);
        }
      }
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("The Master has shutdown, I'll shutdown now.");
      }
      owner.toShutdown = true;
      owner.abruptShutdown = true;
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

  /**
   * usage.
   * */
  static final String USAGE = "Usage: worker [--conf <conf_dir>]";

  /**
   * {@link ExecutorService} for query executions.
   * */
  private volatile ThreadPoolExecutor queryExecutor;

  /**
   * @return the query executor used in this worker.
   * */
  ExecutorService getQueryExecutor() {
    return queryExecutor;
  }

  /**
   * {@link ExecutorService} for non-query message processing.
   * */
  private volatile ExecutorService messageProcessingExecutor;

  /**
   * current active queries. queryID -> QueryPartition
   * */
  private final ConcurrentHashMap<Long, WorkerQueryPartition> activeQueries;

  /**
   * IPC flow controller.
   * */
  private final FlowControlHandler flowController;

  /**
   * My message handler.
   * */
  private final ShortMessageProcessor<TransportMessage> workerShortMessageProcessor;

  /**
   * timer task executor.
   * */
  private ScheduledExecutorService scheduledTaskExecutor;

  /**
   * The ID of this worker.
   */
  private final int myID;

  /**
   * connectionPool[0] is always the master.
   */
  private final IPCConnectionPool connectionPool;

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
  private final LinkedBlockingQueue<ControlMessage> controlMessageQueue;

  /**
   * Message queue for queries.
   * */
  private final PriorityBlockingQueue<WorkerQueryPartition> queryQueue;

  /**
   * My catalog.
   * */
  private final WorkerCatalog catalog;

  /**
   * master IPC address.
   * */
  private final SocketInfo masterSocketInfo;

  /**
   * Query execution mode. May remove
   * */
  private final QueryExecutionMode queryExecutionMode;

  /**
   * Producer channel mapping of current active queries.
   * */
  private final ConcurrentHashMap<StreamIOChannelID, StreamOutputChannel<TransportMessage>> producerChannelMapping;

  /**
   * {@link ExecutorService} for Netty pipelines.
   * */
  private volatile OrderedMemoryAwareThreadPoolExecutor pipelineExecutor;

  /**
   * The default input buffer capacity for each {@link Consumer} input buffer.
   * */
  private final int inputBufferCapacity;

  /**
   * the system wide default inuput buffer recover event trigger.
   * 
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   * */
  private final int inputBufferRecoverTrigger;

  /**
   * Current working directory. It's the logical root of the worker. All the data the worker and the operators running
   * on the worker can access should be put under this directory.
   * */
  private final String workingDirectory;

  /**
   * Execution environment variables for operators.
   * */
  private final ConcurrentHashMap<String, Object> execEnvVars;

  /**
   * Worker process entry point.
   * 
   * @param args command line arguments.
   * */
  public static void main(final String[] args) {
    try {
      java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
      java.util.logging.Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

      if (args.length > 2) {
        LOGGER.warn("Invalid number of arguments.\n" + USAGE);
        JVMUtils.shutdownVM();
      }

      String workingDir = null;
      if (args.length >= 2) {
        if (args[0].equals("--workingDir")) {
          workingDir = args[1];
        } else {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Invalid arguments.\n" + USAGE);
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

  /**
   * @return my control message queue.
   * */
  LinkedBlockingQueue<ControlMessage> getControlMessageQueue() {
    return controlMessageQueue;
  }

  /**
   * @return my query queue.
   * */
  PriorityBlockingQueue<WorkerQueryPartition> getQueryQueue() {
    return queryQueue;
  }

  /**
   * @return my flow controller.
   * */
  FlowControlHandler getFlowControlHandler() {
    return flowController;
  }

  /**
   * @return my connection pool for IPC.
   * */
  IPCConnectionPool getIPCConnectionPool() {
    return connectionPool;
  }

  /**
   * @return my pipeline executor.
   * */
  OrderedMemoryAwareThreadPoolExecutor getPipelineExecutor() {
    return pipelineExecutor;
  }

  /**
   * @return the system wide default inuput buffer capacity.
   * */
  int getInputBufferCapacity() {
    return inputBufferCapacity;
  }

  /**
   * @return the system wide default inuput buffer recover event trigger.
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   * */
  int getInputBufferRecoverTrigger() {
    return inputBufferRecoverTrigger;
  }

  /**
   * @return my execution environment variables for init of operators.
   * */
  ConcurrentHashMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * @return the working directory of the worker.
   * */
  String getWorkingDirectory() {
    return workingDirectory;
  }

  /**
   * @return the current active queries.
   * */
  ConcurrentHashMap<Long, WorkerQueryPartition> getActiveQueries() {
    return activeQueries;
  }

  /**
   * @return query execution mode.
   * */
  QueryExecutionMode getQueryExecutionMode() {
    return queryExecutionMode;
  }

  /**
   * @param workingDirectory my working directory.
   * @param mode my execution mode.
   * @throws CatalogException if there's any catalog operation errors.
   * @throws FileNotFoundException if catalog files are not found.
   * */
  public Worker(final String workingDirectory, final QueryExecutionMode mode) throws CatalogException,
      FileNotFoundException {
    queryExecutionMode = mode;
    catalog = WorkerCatalog.open(FilenameUtils.concat(workingDirectory, "worker.catalog"));

    this.workingDirectory = workingDirectory;
    myID = Integer.parseInt(catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_IDENTIFIER));

    controlMessageQueue = new LinkedBlockingQueue<ControlMessage>();
    queryQueue = new PriorityBlockingQueue<WorkerQueryPartition>();

    masterSocketInfo = catalog.getMasters().get(0);
    workerShortMessageProcessor = new WorkerShortMessageProcessor(this);

    final Map<Integer, SocketInfo> workers = catalog.getWorkers();
    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(MyriaConstants.MASTER_ID, masterSocketInfo);

    connectionPool =
        new IPCConnectionPool(myID, computingUnits, IPCConfigurations.createWorkerIPCServerBootstrap(this),
            IPCConfigurations.createWorkerIPCClientBootstrap(this));
    activeQueries = new ConcurrentHashMap<Long, WorkerQueryPartition>();
    producerChannelMapping = new ConcurrentHashMap<StreamIOChannelID, StreamOutputChannel<TransportMessage>>();
    flowController = new FlowControlHandler(null, producerChannelMapping);

    inputBufferCapacity =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));

    inputBufferRecoverTrigger =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER));

    execEnvVars = new ConcurrentHashMap<String, Object>();

    for (Entry<String, String> cE : catalog.getAllConfigurations().entrySet()) {
      execEnvVars.put(cE.getKey(), cE.getValue());
    }
    final String databaseType = catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_STORAGE_SYSTEM_TYPE);
    switch (databaseType) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        String sqliteFilePath = catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_DATA_SQLITE_DB);
        execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_SQLITE_FILE, sqliteFilePath);
        SQLiteConnection conn = new SQLiteConnection(new File(sqliteFilePath));
        try {
          conn.open(true);
          /* By default, use WAL */
          conn.exec("PRAGMA journal_mode=WAL;");
        } catch (SQLiteException e) {
          e.printStackTrace();
        }
        conn.dispose();
        break;
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        /* TODO fill this in. */
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        /* TODO fill this in. */
        break;
      default:
        throw new CatalogException("Unknown worker type: " + databaseType);
    }

    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_IPC_CONNECTION_POOL, connectionPool);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, queryExecutionMode);

  }

  /**
   * It does the initialization and preparation for the execution of the query.
   * 
   * @param query the received query.
   * @throws DbException if any error occurs.
   */
  public void receiveQuery(final WorkerQueryPartition query) throws DbException {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Query received" + query.getQueryID());
    }

    activeQueries.put(query.getQueryID(), query);
    query.getExecutionFuture().addListener(new QueryFutureListener() {

      @Override
      public void operationComplete(final QueryFuture future) throws Exception {
        activeQueries.remove(query.getQueryID());

        if (future.isSuccess()) {

          sendMessageToMaster(IPCUtils.queryCompleteTM(query.getQueryID(), query.getExecutionStatistics()))
              .addListener(new ChannelFutureListener() {

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

        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Query failed because of exception: ", future.getCause());
          }

          sendMessageToMaster(
              IPCUtils.queryFailureTM(query.getQueryID(), future.getCause(), query.getExecutionStatistics()))
              .addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                  if (future.isSuccess()) {
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug("The query complete message is sent to the master for sure ");
                    }
                  }
                }

              });

        }
      }
    });
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
        new OrderedMemoryAwareThreadPoolExecutor(3, 0, 0, MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS,
            TimeUnit.MILLISECONDS, new RenamingThreadFactory("Pipeline executor"));

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
        workerInJVMPipelineFactory, new InJVMLoopbackChannelSink(), workerShortMessageProcessor,
        new TransportMessageSerializer());

    if (queryExecutionMode == QueryExecutionMode.NON_BLOCKING) {
      int numCPU = Runtime.getRuntime().availableProcessors();
      queryExecutor =
          new ThreadPoolExecutor(numCPU, numCPU, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
              new RenamingThreadFactory("Nonblocking query executor"));
    } else {
      // blocking query execution
      queryExecutor =
          (ThreadPoolExecutor) Executors.newCachedThreadPool(new RenamingThreadFactory("Blocking query executor"));
    }
    messageProcessingExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Control/Query message processor"));
    messageProcessingExecutor.submit(new QueryMessageProcessor());
    messageProcessingExecutor.submit(new ControlMessageProcessor());
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(new RenamingThreadFactory("Worker global timer"));
    scheduledTaskExecutor.scheduleAtFixedRate(new ShutdownChecker(), SHUTDOWN_CHECKER_INTERVAL_MS,
        SHUTDOWN_CHECKER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(new Reporter(this), Reporter.INITIAL_DELAY_IN_MS, Reporter.INTERVAL,
        TimeUnit.MILLISECONDS);
    /* Tell the master we're alive. */
    sendMessageToMaster(IPCUtils.CONTROL_WORKER_ALIVE).awaitUninterruptibly();
  }

  /**
   * The time interval in milliseconds for check if the worker should be shutdown.
   * */
  static final int SHUTDOWN_CHECKER_INTERVAL_MS = 500;

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
