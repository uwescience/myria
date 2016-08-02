package edu.washington.escience.myria.parallel;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.io.FilenameUtils;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.tools.MyriaConfiguration;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.JVMUtils;
import edu.washington.escience.myria.util.concurrent.ErrorLoggingTimerTask;
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;
import edu.washington.escience.myria.util.concurrent.ThreadAffinityFixedRoundRobinExecutionPool;

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
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Worker.class);

  /**
   * Control message processor.
   */
  private final class ControlMessageProcessor implements Runnable {
    @Override
    public void run() {
      try {

        TERMINATE_MESSAGE_PROCESSING:
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
            break TERMINATE_MESSAGE_PROCESSING;
          }

          ControlMessage cm = null;
          try {
            while ((cm = controlMessageQueue.take()) != null) {
              int workerId = cm.getWorkerId();
              switch (cm.getType()) {
                case SHUTDOWN:
                  if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("shutdown requested");
                  }
                  toShutdown = true;
                  abruptShutdown = false;
                  break;
                case REMOVE_WORKER:
                  if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("received REMOVE_WORKER " + workerId);
                  }
                  connectionPool.removeRemote(workerId).await();
                  sendMessageToMaster(IPCUtils.removeWorkerAckTM(workerId));
                  for (WorkerSubQuery wqp : executingSubQueries.values()) {
                    if (wqp.getFTMode().equals(FTMode.ABANDON)) {
                      wqp.getMissingWorkers().add(workerId);
                      wqp.updateProducerChannels(workerId, false);
                      wqp.triggerFragmentEosEoiChecks();
                    } else if (wqp.getFTMode().equals(FTMode.REJOIN)) {
                      wqp.getMissingWorkers().add(workerId);
                    }
                  }
                  break;
                case ADD_WORKER:
                  if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("received ADD_WORKER " + workerId);
                  }
                  connectionPool.putRemote(
                      workerId, SocketInfo.fromProtobuf(cm.getRemoteAddress()));
                  sendMessageToMaster(IPCUtils.addWorkerAckTM(workerId));
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
   * Query related command from master.
   * */
  static final class QueryCommand {
    /**
     * @param q the target query.
     * @param queryMsg the command message.
     * */
    public QueryCommand(final WorkerSubQuery q, final QueryMessage queryMsg) {
      this.q = q;
      this.queryMsg = queryMsg;
    }

    /**
     * the target query.
     * */
    private final WorkerSubQuery q;

    /**
     * the command message.
     * */
    private final QueryMessage queryMsg;
  }
  /**
   * The non-blocking query driver. It calls root.nextReady() to start a query.
   */
  private class QueryMessageProcessor implements Runnable {

    @Override
    public final void run() {
      try {
        QueryCommand q = null;
        while (true) {
          try {
            q = queryQueue.take();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }

          switch (q.queryMsg.getType()) {
            case QUERY_START:
              q.q.init();
              q.q.startExecution();
              break;
            case QUERY_KILL:
              q.q.kill();
              break;
            case QUERY_RECOVER:
              if (q.q.getFTMode().equals(FTMode.REJOIN)) {
                q.q.addRecoveryTasks(q.queryMsg.getWorkerId());
              }
              break;
            case QUERY_DISTRIBUTE:
              try {
                receiveQuery(q.q);
                sendMessageToMaster(IPCUtils.queryReadyTM(q.q.getSubQueryId()));
              } catch (DbException e) {
                if (LOGGER.isErrorEnabled()) {
                  LOGGER.error("Unexpected exception at preparing query. Drop the query.", e);
                }
                q = null;
              }

              break;
            default:
              break;
          }
        }
      } catch (Throwable ee) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown exception caught at query nonblocking driver.", ee);
        }
      }
    }
  }

  /** Send heartbeats to server periodically. */
  private class HeartbeatReporter extends ErrorLoggingTimerTask {
    @Override
    public synchronized void runInner() {
      LOGGER.trace("sending heartbeat to server");
      sendMessageToMaster(IPCUtils.CONTROL_WORKER_HEARTBEAT).awaitUninterruptibly();
    }
  }

  /**
   * Periodically detect whether the {@link Worker} should be shutdown. 1) it detects whether the server is still alive.
   * If the server got killed because of any reason, the workers will be terminated. 2) it detects whether a shutdown
   * message is received.
   */
  private class ShutdownChecker extends ErrorLoggingTimerTask {
    @Override
    public final synchronized void runInner() {
      try {
        if (!connectionPool.isRemoteAlive(MyriaConstants.MASTER_ID)) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("The Master has shutdown, I'll shutdown now.");
          }
          toShutdown = true;
          abruptShutdown = true;
        }
      } catch (Throwable e) {
        toShutdown = true;
        abruptShutdown = true;
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown error in " + ShutdownChecker.class.getSimpleName(), e);
        }
      }
      if (toShutdown) {
        try {
          shutdown();
        } catch (Throwable e) {
          try {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Unknown error in shutdown, halt the worker directly", e);
            }
          } finally {
            JVMUtils.shutdownVM();
          }
        }
      }
    }
  }

  /**
   * usage.
   */
  static final String USAGE = "Usage: worker [--conf <conf_dir>]";

  /**
   * {@link ExecutorService} for query executions.
   */
  private volatile ExecutorService queryExecutor;

  /**
   * @return the query executor used in this worker.
   */
  ExecutorService getQueryExecutor() {
    return queryExecutor;
  }

  /**
   * {@link ExecutorService} for non-query message processing.
   */
  private volatile ExecutorService messageProcessingExecutor;

  /** Currently active queries. Query ID -> {@link SubQueryId}. */
  private final Map<Long, SubQueryId> activeQueries;
  /** Currently running subqueries. {@link SubQueryId} -> {@link WorkerSubQuery}. */
  private final Map<SubQueryId, WorkerSubQuery> executingSubQueries;

  /**
   * shutdown checker executor.
   */
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
   */
  private volatile boolean abruptShutdown = false;

  /**
   * Message queue for control messages.
   */
  private final LinkedBlockingQueue<ControlMessage> controlMessageQueue;

  /**
   * Message queue for queries.
   */
  private final LinkedBlockingQueue<QueryCommand> queryQueue;

  /** Worker configuration. */
  private final MyriaConfiguration config;

  /**
   * Query execution mode. May remove
   */
  private final QueryExecutionMode queryExecutionMode;

  /**
   * {@link ExecutorService} for Netty pipelines.
   */
  private volatile OrderedMemoryAwareThreadPoolExecutor pipelineExecutor;

  /**
   * Current working directory. It's the logical root of the worker. All the data the worker and the operators running
   * on the worker can access should be put under this directory.
   */
  private final String workingDirectory;

  /**
   * Execution environment variables for operators.
   */
  private final ConcurrentHashMap<String, Object> execEnvVars;

  /**
   * The thread group of the main thread.
   */
  private static volatile ThreadGroup mainThreadGroup;

  /**
   * The profiling logger for this worker.
   */
  @GuardedBy("this")
  private ProfilingLogger profilingLogger;
  /**
   * The pythonUDF registry for this worker.
   */
  @GuardedBy("this")
  private PythonFunctionRegistrar pythonFunctionRegistrar;

  /**
   * @param args command line arguments
   * @return options parsed from command line.
   */
  private static HashMap<String, Object> processArgs(final String[] args) {
    HashMap<String, Object> options = new HashMap<String, Object>();
    // if (args.length > 2) {
    // LOGGER.warn("Invalid number of arguments.\n" + USAGE);
    // JVMUtils.shutdownVM();
    // }

    String workingDirTmp = System.getProperty("user.dir");
    if (args.length >= 2) {
      if (args[0].equals("--workingDir")) {
        workingDirTmp = args[1];
      } else {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Invalid arguments.\n" + USAGE);
        }
        JVMUtils.shutdownVM();
      }
    }
    options.put("workingDir", workingDirTmp);
    return options;
  }

  /**
   * Setup system properties.
   *
   * @param cmdlineOptions command line options
   */
  private static void systemSetup(final HashMap<String, Object> cmdlineOptions) {
    java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    java.util.logging.Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    Thread.setDefaultUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(final Thread t, final Throwable e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Uncaught exception in thread: " + t, e);
            }
            if (e instanceof OutOfMemoryError) {
              JVMUtils.shutdownVM();
            }
          }
        });
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                systemCleanup();
              }
            });

    mainThreadGroup = Thread.currentThread().getThreadGroup();
  }

  /**
   * A file lock hold by a worker process throughout its lifetime to make sure no other worker instances start.
   */
  private static volatile FileLock workerInstanceLock;

  /**
   * @param cmdlineOptions command line options
   * @return if a worker instance using the same working directory is already running
   */
  private static boolean workerExists(final HashMap<String, Object> cmdlineOptions) {
    final String workingDir = (String) cmdlineOptions.get("workingDir");
    File file = new File(workingDir + "/workerInstance.lock");
    file.deleteOnExit();
    try {
      @SuppressWarnings("resource")
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      workerInstanceLock = raf.getChannel().tryLock();
    } catch (Throwable e) {
      LOGGER.error("Error in locking worker instance lock", e);
      return true;
    }
    return workerInstanceLock == null;
  }

  /**
   * Cleanup system resources.
   */
  private static void systemCleanup() {
    Throwable err = null;
    if (workerInstanceLock != null) {
      try {
        workerInstanceLock.release();
      } catch (Throwable t) {
        err = t;
      } finally {
        try {
          workerInstanceLock.channel().close();
        } catch (Throwable t) {
          err = t;
        }
      }
    }
    if (err != null) {
      LOGGER.error("Error in system cleanup: ", err);
    }
  }

  /**
   * @param cmdlineOptions command line options
   */
  private static void bootupWorker(final HashMap<String, Object> cmdlineOptions) {
    final String workingDir = (String) cmdlineOptions.get("workingDir");
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("workingDir: " + workingDir);
    }

    ThreadGroup workerThreadGroup = new ThreadGroup(mainThreadGroup, "MyriaWorkerThreadGroup");
    Thread myriaWorkerMain =
        new Thread(workerThreadGroup, "MyriaWorkerMain") {
          @Override
          public void run() {
            try {
              // Instantiate a new worker
              final Worker w = new Worker(workingDir, QueryExecutionMode.NON_BLOCKING);
              // int port = w.port;

              // Start the actual message handler by binding
              // the acceptor to a network socket
              // Now the worker can accept messages
              w.start();

              if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Worker started at:" + w.config.getHostPort(w.myID));
              }
            } catch (Throwable e) {
              if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Unknown error occurs at Worker. Quit directly.", e);
              }
              JVMUtils.shutdownVM();
            }
          }
        };
    myriaWorkerMain.start();
  }

  /**
   * Worker process entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {
    try {
      HashMap<String, Object> cmdlineOptions = processArgs(args);
      if (workerExists(cmdlineOptions)) {
        throw new Exception(
            "Another worker instance with the same configurations already running. Exit directly.");
      }
      systemSetup(cmdlineOptions);
      bootupWorker(cmdlineOptions);
    } catch (Throwable e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unknown error occurs at Worker. Quit directly.", e);
      }
      JVMUtils.shutdownVM();
    }
  }

  /**
   * @return my control message queue.
   */
  LinkedBlockingQueue<ControlMessage> getControlMessageQueue() {
    return controlMessageQueue;
  }

  /**
   * @return my query queue.
   */
  BlockingQueue<QueryCommand> getQueryQueue() {
    return queryQueue;
  }

  /**
   * @return my connection pool for IPC.
   */
  IPCConnectionPool getIPCConnectionPool() {
    return connectionPool;
  }

  /**
   * @return my pipeline executor.
   */
  OrderedMemoryAwareThreadPoolExecutor getPipelineExecutor() {
    return pipelineExecutor;
  }

  /**
   * @return my execution environment variables for init of operators.
   */
  ConcurrentHashMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * @return the working directory of the worker.
   */
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  /**
   * @return the current active queries.
   */
  Map<SubQueryId, WorkerSubQuery> getActiveQueries() {
    return executingSubQueries;
  }

  /**
   * @return query execution mode.
   */
  QueryExecutionMode getQueryExecutionMode() {
    return queryExecutionMode;
  }

  /**
   * @param workingDirectory my working directory.
   * @param mode my execution mode.
   * @throws ConfigFileException if there's any config file parsing error.
   */
  public Worker(final String workingDirectory, final QueryExecutionMode mode)
      throws ConfigFileException {
    queryExecutionMode = mode;
    this.workingDirectory = workingDirectory;
    controlMessageQueue = new LinkedBlockingQueue<ControlMessage>();
    queryQueue = new LinkedBlockingQueue<QueryCommand>();
    activeQueries = new ConcurrentHashMap<>();
    executingSubQueries = new ConcurrentHashMap<>();
    execEnvVars = new ConcurrentHashMap<String, Object>();

    config =
        MyriaConfiguration.loadWithDefaultValues(
            FilenameUtils.concat(workingDirectory, "worker.cfg"));

    myID = Integer.parseInt(config.getRequired("runtime", MyriaSystemConfigKeys.WORKER_IDENTIFIER));

    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(
        MyriaConstants.MASTER_ID, SocketInfo.valueOf(config.getHostPort(MyriaConstants.MASTER_ID)));
    for (int id : config.getWorkerIds()) {
      computingUnits.put(id, SocketInfo.valueOf(config.getHostPort(id)));
    }

    int inputBufferCapacity =
        Integer.valueOf(
            config.getRequired("runtime", MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));
    int inputBufferRecoverTrigger =
        Integer.valueOf(
            config.getRequired(
                "runtime", MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER));
    connectionPool =
        new IPCConnectionPool(
            myID,
            computingUnits,
            IPCConfigurations.createWorkerIPCServerBootstrap(this),
            IPCConfigurations.createWorkerIPCClientBootstrap(this),
            new TransportMessageSerializer(),
            new WorkerShortMessageProcessor(this),
            inputBufferCapacity,
            inputBufferRecoverTrigger);

    final String databaseSystem =
        config.getRequired("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM, databaseSystem);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_NODE_ID, getID());
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, queryExecutionMode);
    LOGGER.info("Worker: Database system " + databaseSystem);
    String jsonConnInfo = config.getSelfJsonConnInfo();
    LOGGER.info("Worker: Connection info " + jsonConnInfo);
    execEnvVars.put(
        MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO,
        ConnectionInfo.of(databaseSystem, jsonConnInfo));
  }

  /**
   * It does the initialization and preparation for the execution of the subquery.
   *
   * @param subQuery the received query.
   * @throws DbException if any error occurs.
   */
  public void receiveQuery(final WorkerSubQuery subQuery) throws DbException {
    final SubQueryId subQueryId = subQuery.getSubQueryId();
    LOGGER.info("SubQuery #{} received.", subQueryId);

    activeQueries.put(subQueryId.getQueryId(), subQueryId);
    executingSubQueries.put(subQueryId, subQuery);
    subQuery
        .getExecutionFuture()
        .addListener(
            new LocalSubQueryFutureListener() {

              @Override
              public void operationComplete(final LocalSubQueryFuture future) {
                finishTask(subQueryId);

                if (future.isSuccess()) {

                  sendMessageToMaster(
                          IPCUtils.queryCompleteTM(subQueryId, subQuery.getExecutionStatistics()))
                      .addListener(
                          new ChannelFutureListener() {

                            @Override
                            public void operationComplete(final ChannelFuture future)
                                throws Exception {
                              if (future.isSuccess()) {
                                if (LOGGER.isDebugEnabled()) {
                                  LOGGER.debug(
                                      "The query complete message is sent to the master for sure ");
                                }
                              }
                            }
                          });
                  LOGGER.info("My part of query {} finished", subQuery);
                } else {
                  LOGGER.error("Query failed because of exception: ", future.getCause());

                  TransportMessage tm = null;
                  try {
                    tm =
                        IPCUtils.queryFailureTM(
                            subQueryId, future.getCause(), subQuery.getExecutionStatistics());
                  } catch (IOException e) {
                    if (LOGGER.isErrorEnabled()) {
                      LOGGER.error("Unknown query failure TM creation error", e);
                    }
                    tm = IPCUtils.simpleQueryFailureTM(subQueryId);
                  }
                  sendMessageToMaster(tm)
                      .addListener(
                          new ChannelFutureListener() {
                            @Override
                            public void operationComplete(final ChannelFuture future)
                                throws Exception {
                              if (future.isSuccess()) {
                                if (LOGGER.isDebugEnabled()) {
                                  LOGGER.debug(
                                      "The query complete message is sent to the master for sure ");
                                }
                              }
                            }
                          });
                }
              }
            });
  }

  /**
   * Finish the subquery by removing it from the data structures.
   *
   * @param subQueryId the id of the subquery to finish.
   */
  private void finishTask(final SubQueryId subQueryId) {
    executingSubQueries.remove(subQueryId);
    activeQueries.remove(subQueryId.getQueryId());
  }

  /**
   * @param message the message to get sent to the master
   * @return the future of this sending action.
   */
  ChannelFuture sendMessageToMaster(final TransportMessage message) {
    return Worker.this.connectionPool.sendShortMessage(MyriaConstants.MASTER_ID, message);
  }

  /**
   * This method should be called whenever the system is going to shutdown.
   *
   */
  void shutdown() {
    LOGGER.info("Shutdown requested. Please wait when cleaning up...");

    for (WorkerSubQuery p : executingSubQueries.values()) {
      p.kill();
    }

    if (!connectionPool.isShutdown()) {
      if (!abruptShutdown) {
        connectionPool.shutdown();
      } else {
        connectionPool.shutdownNow();
      }
    }
    connectionPool.releaseExternalResources();

    if (pipelineExecutor != null && !pipelineExecutor.isShutdown()) {
      pipelineExecutor.shutdown();
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("shutdown IPC completed");
    }
    // must use shutdownNow here because the query queue processor and the control message processor are both
    // blocking.
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
   */
  public void start() throws Exception {
    ExecutorService bossExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("IPC boss"));
    ExecutorService workerExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("IPC worker"));
    pipelineExecutor = null; // Remove pipeline executors
    // new OrderedMemoryAwareThreadPoolExecutor(3, 5 * MyriaConstants.MB, 0,
    // MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS, TimeUnit.MILLISECONDS, new RenamingThreadFactory(
    // "Pipeline executor"));

    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(
            bossExecutor, workerExecutor, Runtime.getRuntime().availableProcessors() * 2 + 1);

    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(
            bossExecutor, workerExecutor, Runtime.getRuntime().availableProcessors() * 2 + 1);

    ChannelPipelineFactory serverPipelineFactory =
        new IPCPipelineFactories.WorkerServerPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory clientPipelineFactory =
        new IPCPipelineFactories.WorkerClientPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory workerInJVMPipelineFactory =
        new IPCPipelineFactories.WorkerInJVMPipelineFactory(connectionPool);

    connectionPool.start(
        serverChannelFactory,
        serverPipelineFactory,
        clientChannelFactory,
        clientPipelineFactory,
        workerInJVMPipelineFactory,
        new InJVMLoopbackChannelSink());

    if (queryExecutionMode == QueryExecutionMode.NON_BLOCKING) {
      int numCPU = Runtime.getRuntime().availableProcessors();
      queryExecutor =
          // new ThreadPoolExecutor(numCPU, numCPU, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
          // new RenamingThreadFactory("Nonblocking query executor"));
          new ThreadAffinityFixedRoundRobinExecutionPool(
              numCPU, new RenamingThreadFactory("Nonblocking query executor"));
    } else {
      // blocking query execution
      queryExecutor =
          Executors.newCachedThreadPool(new RenamingThreadFactory("Blocking query executor"));
    }
    messageProcessingExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Control/Query message processor"));
    messageProcessingExecutor.submit(new QueryMessageProcessor());
    messageProcessingExecutor.submit(new ControlMessageProcessor());
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    scheduledTaskExecutor =
        Executors.newScheduledThreadPool(2, new RenamingThreadFactory("Worker global timer"));
    scheduledTaskExecutor.scheduleAtFixedRate(
        new ShutdownChecker(),
        MyriaConstants.WORKER_SHUTDOWN_CHECKER_INTERVAL,
        MyriaConstants.WORKER_SHUTDOWN_CHECKER_INTERVAL,
        TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(
        new HeartbeatReporter(), 0, MyriaConstants.HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
  }

  /**
   * @param configKey config key.
   * @return a worker runtime configuration value.
   * @throws ConfigFileException if error occurred.
   */
  public String getRuntimeConfiguration(final String configKey) throws ConfigFileException {
    return config.getRequired("runtime", configKey);
  }

  /**
   * @return This worker's ID.
   */
  public int getID() {
    return myID;
  }

  /**
   * @return the profilingLogger
   * @throws DbException if there is an error initializing the profiling logger
   */
  public synchronized PythonFunctionRegistrar getPythonFunctionRegistrar() throws DbException {
    if (pythonFunctionRegistrar == null || !pythonFunctionRegistrar.isValid()) {
      pythonFunctionRegistrar = null;
      ConnectionInfo connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
      if (connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        pythonFunctionRegistrar = new PythonFunctionRegistrar(connectionInfo);
      }
    }
    return pythonFunctionRegistrar;
  }

  /**
   * @return the profilingLogger
   * @throws DbException if there is an error initializing the profiling logger
   */
  public synchronized ProfilingLogger getProfilingLogger() throws DbException {
    if (profilingLogger == null || !profilingLogger.isValid()) {
      profilingLogger = null;
      ConnectionInfo connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
      if (connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        profilingLogger = new ProfilingLogger(connectionInfo);
      }
    }
    return profilingLogger;
  }
}
