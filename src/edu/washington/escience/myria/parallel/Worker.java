package edu.washington.escience.myria.parallel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.google.common.util.concurrent.Striped;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.DefaultStorageDbPassword;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.DefaultStorageDbPort;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.MasterHost;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.MasterRpcPort;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.OperatorInputBufferCapacity;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.OperatorInputBufferRecoverTrigger;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.StorageDbms;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.WorkerConf;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule.WorkerFilesystemPath;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule.WorkerHost;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule.WorkerId;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule.WorkerPort;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule.WorkerStorageDbName;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;
import edu.washington.escience.myria.util.concurrent.ThreadAffinityFixedRoundRobinExecutionPool;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;

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
/**
 *
 */
@Unit
public final class Worker implements Task, TaskMessageSource {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Worker.class);

  private final CountDownLatch terminated = new CountDownLatch(1);

  /**
   * @param memento the memento object passed down by the driver.
   * @return the user defined return value
   * @throws Exception whenever the Task encounters an unsolved issue. This Exception will be thrown at the Driver's
   *           event handler.
   */
  @Override
  public byte[] call(@SuppressWarnings("unused") final byte[] memento) throws Exception {
    try {
      start();
      terminated.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      shutdown();
    }
    return null;
  }

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
   */
  static final class QueryCommand {
    /**
     * @param q the target query.
     * @param queryMsg the command message.
     */
    public QueryCommand(final WorkerSubQuery q, final QueryMessage queryMsg) {
      this.q = q;
      this.queryMsg = queryMsg;
    }

    /**
     * the target query.
     */
    private final WorkerSubQuery q;

    /**
     * the command message.
     */
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

  private final Queue<TaskMessage> pendingDriverMessages = new ConcurrentLinkedQueue<>();

  private Optional<TaskMessage> dequeueDriverMessage() {
    return Optional.ofNullable(pendingDriverMessages.poll());
  }

  private void enqueueDriverMessage(@Nonnull final TransportMessage msg) {
    final TaskMessage driverMsg = TaskMessage.from(myID + "", msg.toByteArray());
    pendingDriverMessages.add(driverMsg);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.reef.task.TaskMessageSource#getMessage()
   *
   * To be used to instruct the driver to launch or abort workers.
   */
  @Override
  public Optional<TaskMessage> getMessage() {
    // TODO: determine which messages should be sent to the driver
    return dequeueDriverMessage();
  }

  private final Striped<Lock> workerAddRemoveLock;

  /**
   * REEF event handler for driver messages indicating worker failure.
   */
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage driverMessage) {
      LOGGER.info("Driver message received");
      TransportMessage m;
      try {
        m = TransportMessage.parseFrom(driverMessage.get().get());
      } catch (InvalidProtocolBufferException e) {
        LOGGER.warn("Could not parse TransportMessage from driver message", e);
        return;
      }
      final ControlMessage controlM = m.getControlMessage();
      LOGGER.info("Control message received: {}", controlM);
      final int workerId = controlM.getWorkerId();
      Lock workerLock = workerAddRemoveLock.get(workerId);
      workerLock.lock();
      try {
        switch (controlM.getType()) {
          case REMOVE_WORKER:
            {
              if (LOGGER.isInfoEnabled()) {
                LOGGER.info("received REMOVE_WORKER for worker " + workerId);
              }
              connectionPool
                  .removeRemote(workerId)
                  .addListener(
                      new ChannelGroupFutureListener() {
                        @Override
                        public void operationComplete(final ChannelGroupFuture future) {
                          if (future.isCompleteSuccess()) {
                            LOGGER.info(
                                "removed connection for remote worker {} from connection pool",
                                workerId);
                          } else {
                            LOGGER.info(
                                "failed to remove connection for remote worker {} from connection pool",
                                workerId);
                          }
                        }
                      });
              for (WorkerSubQuery wqp : executingSubQueries.values()) {
                if (wqp.getFTMode().equals(FTMode.ABANDON)) {
                  wqp.getMissingWorkers().add(workerId);
                  wqp.updateProducerChannels(workerId, false);
                  wqp.triggerFragmentEosEoiChecks();
                } else if (wqp.getFTMode().equals(FTMode.REJOIN)) {
                  wqp.getMissingWorkers().add(workerId);
                }
              }
              enqueueDriverMessage(IPCUtils.removeWorkerAckTM(workerId));
            }
            break;
          case ADD_WORKER:
            {
              if (LOGGER.isInfoEnabled()) {
                LOGGER.info("received ADD_WORKER " + workerId);
              }
              connectionPool.putRemote(
                  workerId, SocketInfo.fromProtobuf(controlM.getRemoteAddress()));
              enqueueDriverMessage(IPCUtils.addWorkerAckTM(workerId));
            }
            break;
          default:
            throw new IllegalStateException(
                "Unexpected driver control message type: " + controlM.getType());
        }
      } finally {
        workerLock.unlock();
      }
    }
  }

  /**
   * Shut down this worker.
   */
  public final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOGGER.info("CloseEvent received, shutting down...");
      terminated.countDown();
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
   * For instantiating nested classes (Tang won't let us use constructors).
   */
  private final Injector injector;

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
    return QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * @param workingDirectory my working directory.
   * @param mode my execution mode.
   * @throws ConfigFileException if there's any config file parsing error.
   */
  @Inject
  public Worker(
      final Injector injector,
      @Parameter(WorkerId.class) final int workerID,
      @Parameter(WorkerHost.class) final String workerHost,
      @Parameter(WorkerPort.class) final int workerPort,
      @Parameter(MasterHost.class) final String masterHost,
      @Parameter(MasterRpcPort.class) final int masterPort,
      @Parameter(StorageDbms.class) final String databaseSystem,
      @Parameter(WorkerStorageDbName.class) final String dbName,
      @Parameter(DefaultStorageDbPassword.class) final String dbPassword,
      @Parameter(DefaultStorageDbPort.class) final int dbPort,
      @Parameter(WorkerFilesystemPath.class) final String rootPath,
      @Parameter(TcpConnectionTimeoutMillis.class) final int connectTimeoutMillis,
      @Parameter(TcpSendBufferSizeBytes.class) final int sendBufferSize,
      @Parameter(TcpReceiveBufferSizeBytes.class) final int receiveBufferSize,
      @Parameter(FlowControlWriteBufferLowMarkBytes.class) final int writeBufferLowWaterMark,
      @Parameter(FlowControlWriteBufferHighMarkBytes.class) final int writeBufferHighWaterMark,
      @Parameter(OperatorInputBufferCapacity.class) final int inputBufferCapacity,
      @Parameter(OperatorInputBufferRecoverTrigger.class) final int inputBufferRecoverTrigger,
      @Parameter(WorkerConf.class) final Set<String> workerConfs)
      throws Exception {

    this.injector = injector;
    myID = workerID;
    final String subDir = FilenameUtils.concat("workers", myID + "");
    workingDirectory = FilenameUtils.concat(rootPath, subDir);
    controlMessageQueue = new LinkedBlockingQueue<ControlMessage>();
    queryQueue = new LinkedBlockingQueue<QueryCommand>();
    activeQueries = new ConcurrentHashMap<>();
    executingSubQueries = new ConcurrentHashMap<>();
    execEnvVars = new ConcurrentHashMap<String, Object>();

    final Map<Integer, SocketInfo> computingUnits =
        getComputingUnits(masterHost, masterPort, workerConfs);

    workerAddRemoveLock = Striped.lock(workerConfs.size());
    connectionPool =
        new IPCConnectionPool(
            myID,
            computingUnits,
            IPCConfigurations.createWorkerIPCServerBootstrap(
                connectTimeoutMillis,
                sendBufferSize,
                receiveBufferSize,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark),
            IPCConfigurations.createWorkerIPCClientBootstrap(
                connectTimeoutMillis,
                sendBufferSize,
                receiveBufferSize,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark),
            new TransportMessageSerializer(),
            new WorkerShortMessageProcessor(this),
            inputBufferCapacity,
            inputBufferRecoverTrigger);

    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM, databaseSystem);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_NODE_ID, getID());
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, getQueryExecutionMode());
    LOGGER.info("Worker: Database system " + databaseSystem);
    String jsonConnInfo =
        ConnectionInfo.toJson(
            databaseSystem, workerHost, workingDirectory, workerID, dbName, dbPassword, dbPort);
    LOGGER.info("Worker: Connection info " + jsonConnInfo);
    execEnvVars.put(
        MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO,
        ConnectionInfo.of(databaseSystem, jsonConnInfo));
  }

  private Map<Integer, SocketInfo> getComputingUnits(
      final String masterHost, final Integer masterPort, final Set<String> serializedWorkerConfs)
      throws BindException, IOException, InjectionException {
    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.put(MyriaConstants.MASTER_ID, new SocketInfo(masterHost, masterPort));
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    for (final String serializedWorkerConf : serializedWorkerConfs) {
      final Configuration workerConf = serializer.fromString(serializedWorkerConf);
      final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
      Integer workerID = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerId.class);
      String workerHost =
          injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
      Integer workerPort =
          injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerPort.class);
      computingUnits.put(workerID, new SocketInfo(workerHost, workerPort));
    }
    return computingUnits;
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
    // must use shutdownNow here because the query queue processor and the control message processor
    // are both
    // blocking.
    // We have to interrupt them at shutdown.
    messageProcessingExecutor.shutdownNow();
    queryExecutor.shutdown();
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
    // MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS, TimeUnit.MILLISECONDS, new
    // RenamingThreadFactory(
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

    if (getQueryExecutionMode() == QueryExecutionMode.NON_BLOCKING) {
      int numCPU = Runtime.getRuntime().availableProcessors();
      queryExecutor =
          // new ThreadPoolExecutor(numCPU, numCPU, 0L, TimeUnit.MILLISECONDS, new
          // LinkedBlockingQueue<Runnable>(),
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
    messageProcessingExecutor.submit(injector.getInstance(QueryMessageProcessor.class));
    messageProcessingExecutor.submit(injector.getInstance(ControlMessageProcessor.class));
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
  /**
   * @return Python function registrar
   * @throws DbException if there is an error initializing the python function registrar
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
}
