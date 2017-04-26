package edu.washington.escience.myria.parallel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Striped;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.PostgresBinaryTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.io.ByteSink;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.UriSink;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.CSVFileScanFragment;
import edu.washington.escience.myria.operator.DbCreateFunction;
import edu.washington.escience.myria.operator.DbCreateIndex;
import edu.washington.escience.myria.operator.DbCreateView;
import edu.washington.escience.myria.operator.DbDelete;
import edu.washington.escience.myria.operator.DbExecute;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DuplicateTBGenerator;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.EmptyRelation;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TupleSink;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregatorFactory;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.distribute.BroadcastDistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.HowDistributed;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myria.parallel.ipc.QueueBasedShortMessageProcessor;
import edu.washington.escience.myria.perfenforce.PerfEnforceDriver;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryReport;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.DefaultInstancePath;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.FlowControlWriteBufferHighMarkBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.FlowControlWriteBufferLowMarkBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.MasterHost;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.MasterRpcPort;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.OperatorInputBufferCapacity;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.OperatorInputBufferRecoverTrigger;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.PersistUri;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.StorageDbms;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpConnectionTimeoutMillis;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpReceiveBufferSizeBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.TcpSendBufferSizeBytes;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.WorkerConf;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.ErrorLoggingTimerTask;
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;

/**
 * The master entrance.
 */
public final class Server implements TaskMessageSource, EventHandler<DriverMessage> {

  /** Master message processor. */
  private final class MessageProcessor implements Runnable {

    /** Constructor, set the thread name. */
    public MessageProcessor() {
      super();
    }

    @Override
    public void run() {
      TERMINATE_MESSAGE_PROCESSING:
      while (true) {
        try {
          IPCMessage.Data<TransportMessage> mw = null;
          try {
            mw = messageQueue.take();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            break TERMINATE_MESSAGE_PROCESSING;
          }

          final TransportMessage m = mw.getPayload();
          final int senderID = mw.getRemoteID();
          switch (m.getType()) {
            case CONTROL:
              final ControlMessage controlM = m.getControlMessage();
              switch (controlM.getType()) {
                case RESOURCE_STATS:
                  queryManager.updateResourceStats(senderID, controlM);
                  break;
                default:
                  LOGGER.error("Unexpected control message received at master: {}", controlM);
                  break;
              }
              break;
            case QUERY:
              final QueryMessage qm = m.getQueryMessage();
              final SubQueryId subQueryId = new SubQueryId(qm.getQueryId(), qm.getSubqueryId());
              switch (qm.getType()) {
                case QUERY_READY_TO_EXECUTE:
                  LOGGER.info("Worker #{} is ready to execute query #{}.", senderID, subQueryId);
                  queryManager.workerReady(subQueryId, senderID);
                  break;
                case QUERY_COMPLETE:
                  QueryReport qr = qm.getQueryReport();
                  if (qr.getSuccess()) {
                    LOGGER.info(
                        "Worker #{} succeeded in executing query #{}.", senderID, subQueryId);
                    queryManager.workerComplete(subQueryId, senderID);
                  } else {
                    ObjectInputStream osis = null;
                    Throwable cause = null;
                    try {
                      osis =
                          new ObjectInputStream(
                              new ByteArrayInputStream(qr.getCause().toByteArray()));
                      cause = (Throwable) (osis.readObject());
                    } catch (IOException | ClassNotFoundException e) {
                      LOGGER.error("Error decoding failure cause", e);
                    }
                    LOGGER.error(
                        "Worker #{} failed in executing query #{}.", senderID, subQueryId, cause);
                    queryManager.workerFailed(subQueryId, senderID, cause);
                  }
                  break;
                default:
                  LOGGER.error("Unexpected query message received at master: {}", qm);
                  break;
              }
              break;
            default:
              LOGGER.error("Unknown short message received at master: {}", m.getType());
              break;
          }
        } catch (Throwable a) {
          LOGGER.error("Error occured in master message processor.", a);
          if (a instanceof Error) {
            throw a;
          }
          if (a instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            break TERMINATE_MESSAGE_PROCESSING;
          }
        }
      }
    }
  }

  private final Queue<TaskMessage> pendingDriverMessages = new ConcurrentLinkedQueue<>();

  private Optional<TaskMessage> dequeueDriverMessage() {
    return Optional.ofNullable(pendingDriverMessages.poll());
  }

  private void enqueueDriverMessage(@Nonnull final TransportMessage msg) {
    final TaskMessage driverMsg =
        TaskMessage.from(MyriaConstants.MASTER_ID + "", msg.toByteArray());
    pendingDriverMessages.add(driverMsg);
  }

  /* (non-Javadoc)
   * @see org.apache.reef.task.TaskMessageSource#getMessage() To be used to instruct the driver to launch or abort
   * workers. */
  @Override
  public Optional<TaskMessage> getMessage() {
    // TODO: determine which messages should be sent to the driver
    return dequeueDriverMessage();
  }

  private Striped<Lock> workerAddRemoveLock;

  /** REEF event handler for driver messages indicating worker failure. */
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
    // We received a failed worker message from the driver.
    final int workerId = controlM.getWorkerId();
    Lock workerLock = workerAddRemoveLock.get(workerId);
    workerLock.lock();
    try {
      switch (controlM.getType()) {
        case REMOVE_WORKER:
          {
            LOGGER.info(
                "Driver reported worker {} as dead, removing from alive workers.", workerId);
            aliveWorkers.remove(workerId);
            queryManager.workerDied(workerId);
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
            enqueueDriverMessage(IPCUtils.removeWorkerAckTM(workerId));
          }
          break;
        case ADD_WORKER:
          {
            Preconditions.checkState(!aliveWorkers.contains(workerId));
            LOGGER.info("Driver wants to add worker {} to alive workers.", workerId);
            connectionPool.putRemote(
                workerId, SocketInfo.fromProtobuf(controlM.getRemoteAddress()));
            queryManager.workerRestarted(
                workerId, ImmutableSet.copyOf(controlM.getAckedWorkerIdsList()));
            aliveWorkers.add(workerId);
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

  /** The usage message for this server. */
  static final String USAGE = "Usage: Server catalogFile [-explain] [-f queryFile]";

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Server.class);

  /** Initial worker list. */
  private ImmutableMap<Integer, SocketInfo> workers = null;

  /** Manages the queries executing in this instance of Myria. */
  private QueryManager queryManager = null;

  /** @return the query manager. */
  public QueryManager getQueryManager() {
    return queryManager;
  }

  /** Current alive worker set. */
  private final Set<Integer> aliveWorkers;

  /** Execution environment variables for operators. */
  private final ConcurrentHashMap<String, Object> execEnvVars;

  /**
   * All message queue.
   *
   * @TODO remove this queue as in {@link Worker}s.
   */
  private final LinkedBlockingQueue<IPCMessage.Data<TransportMessage>> messageQueue;

  /** The IPC Connection Pool. */
  private IPCConnectionPool connectionPool;

  /** {@link ExecutorService} for message processing. */
  private volatile ExecutorService messageProcessingExecutor;

  /** The Catalog stores the metadata about the Myria instance. */
  private MasterCatalog catalog;

  /**
   * The {@link OrderedMemoryAwareThreadPoolExecutor} who gets messages from {@link workerExecutor} and further process
   * them using application specific message handlers, e.g. {@link MasterShortMessageProcessor}.
   */
  private volatile OrderedMemoryAwareThreadPoolExecutor ipcPipelineExecutor;

  /** The {@link ExecutorService} who executes the master-side subqueries. */
  private volatile ExecutorService serverQueryExecutor;

  /** Absolute path of the directory containing the master catalog files */
  private final String catalogPath;

  /** The URI to persist relations */
  private final String persistURI;

  /** @return the query executor used in this worker. */
  ExecutorService getQueryExecutor() {
    return serverQueryExecutor;
  }

  /** max number of seconds for elegant cleanup. */
  public static final int NUM_SECONDS_FOR_ELEGANT_CLEANUP = 10;

  /** @return my connection pool for IPC. */
  IPCConnectionPool getIPCConnectionPool() {
    return connectionPool;
  }

  /** @return my pipeline executor. */
  OrderedMemoryAwareThreadPoolExecutor getPipelineExecutor() {
    return ipcPipelineExecutor;
  }

  /** The socket info for the master. */
  private final SocketInfo masterSocketInfo;

  /** The PerfEnforceDriver */
  private PerfEnforceDriver perfEnforceDriver;

  /**
   * @return my execution environment variables for init of operators.
   */
  ConcurrentHashMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /** @return execution mode. */
  QueryExecutionMode getExecutionMode() {
    return QueryExecutionMode.NON_BLOCKING;
  }

  private final String instancePath;
  private final int connectTimeoutMillis;
  private final int sendBufferSize;
  private final int receiveBufferSize;
  private final int writeBufferLowWaterMark;
  private final int writeBufferHighWaterMark;
  private final int inputBufferCapacity;
  private final int inputBufferRecoverTrigger;
  private final Injector injector;

  /**
   * Construct a server object, with configuration stored in the specified catalog file.
   *
   * @param masterHost hostname of the master
   * @param masterPort RPC port of the master
   * @param instancePath absolute path of the directory containing the master catalog files
   * @param databaseSystem name of the storage DB system
   * @param connectTimeoutMillis connect timeout for worker IPC
   * @param sendBufferSize send buffer size in bytes for worker IPC
   * @param receiveBufferSize receive buffer size in bytes for worker IPC
   * @param writeBufferLowWaterMark low watermark for write buffer overflow recovery
   * @param writeBufferHighWaterMark high watermark for write buffer overflow recovery
   * @param inputBufferCapacity size of the input buffer in bytes
   * @param inputBufferRecoverTrigger number of bytes in the input buffer to trigger recovery after overflow
   * @param persistURI the storage endpoint URI for persisting partitioned relations
   * @param injector a Tang injector for instantiating objects from configuration
   */
  @Inject
  public Server(
      @Parameter(MasterHost.class) final String masterHost,
      @Parameter(MasterRpcPort.class) final int masterPort,
      @Parameter(DefaultInstancePath.class) final String instancePath,
      @Parameter(StorageDbms.class) final String databaseSystem,
      @Parameter(TcpConnectionTimeoutMillis.class) final int connectTimeoutMillis,
      @Parameter(TcpSendBufferSizeBytes.class) final int sendBufferSize,
      @Parameter(TcpReceiveBufferSizeBytes.class) final int receiveBufferSize,
      @Parameter(FlowControlWriteBufferLowMarkBytes.class) final int writeBufferLowWaterMark,
      @Parameter(FlowControlWriteBufferHighMarkBytes.class) final int writeBufferHighWaterMark,
      @Parameter(OperatorInputBufferCapacity.class) final int inputBufferCapacity,
      @Parameter(OperatorInputBufferRecoverTrigger.class) final int inputBufferRecoverTrigger,
      @Parameter(PersistUri.class) final String persistURI,
      final Injector injector) {

    this.instancePath = instancePath;
    this.connectTimeoutMillis = connectTimeoutMillis;
    this.sendBufferSize = sendBufferSize;
    this.receiveBufferSize = receiveBufferSize;
    this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    this.inputBufferCapacity = inputBufferCapacity;
    this.inputBufferRecoverTrigger = inputBufferRecoverTrigger;
    this.persistURI = persistURI;
    this.injector = injector;

    masterSocketInfo = new SocketInfo(masterHost, masterPort);
    this.catalogPath = instancePath;

    execEnvVars = new ConcurrentHashMap<>();
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_NODE_ID, MyriaConstants.MASTER_ID);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, getExecutionMode());
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM, databaseSystem);

    aliveWorkers = Sets.newConcurrentHashSet();
    messageQueue = new LinkedBlockingQueue<>();
  }

  /** timer task executor. */
  private ScheduledExecutorService scheduledTaskExecutor;

  /** This class presents only for the purpose of debugging. No other usage. */
  private class DebugHelper extends ErrorLoggingTimerTask {

    /** Interval of execution. */
    public static final int INTERVAL = MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS;

    @Override
    public final synchronized void runInner() {
      System.currentTimeMillis();
    }
  }

  private ImmutableSet<Configuration> getWorkerConfs(final Injector injector)
      throws InjectionException, BindException, IOException {
    final ImmutableSet.Builder<Configuration> workerConfsBuilder = new ImmutableSet.Builder<>();
    final Set<String> serializedWorkerConfs = injector.getNamedInstance(WorkerConf.class);
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    for (final String serializedWorkerConf : serializedWorkerConfs) {
      final Configuration workerConf = serializer.fromString(serializedWorkerConf);
      workerConfsBuilder.add(workerConf);
    }
    return workerConfsBuilder.build();
  }

  private static Integer getIdFromWorkerConf(final Configuration workerConf)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerId.class);
  }

  private static String getHostFromWorkerConf(final Configuration workerConf)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
  }

  private static Integer getPortFromWorkerConf(final Configuration workerConf)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerPort.class);
  }

  /** Master cleanup. */
  private void cleanup() {
    LOGGER.info("{} is going to shutdown", MyriaConstants.SYSTEM_NAME);

    queryManager.killAll();

    if (messageProcessingExecutor != null && !messageProcessingExecutor.isShutdown()) {
      messageProcessingExecutor.shutdownNow();
    }
    if (scheduledTaskExecutor != null && !scheduledTaskExecutor.isShutdown()) {
      scheduledTaskExecutor.shutdownNow();
    }

    /* Close the catalog before shutting down the IPC because there may be Catalog jobs pending that were triggered by
     * IPC events. */
    catalog.close();

    connectionPool.shutdown();
    connectionPool.releaseExternalResources();
    if (ipcPipelineExecutor != null && !ipcPipelineExecutor.isShutdown()) {
      ipcPipelineExecutor.shutdown();
    }
    LOGGER.info("Master connection pool shutdown complete.");

    LOGGER.info("Master finishes cleanup.");
  }

  /** Shutdown the master. */
  public void shutdown() {
    cleanup();
  }

  /**
   * Start all the threads that do work for the server.
   *
   * @throws Exception if any error occurs.
   */
  public void start() throws Exception {
    LOGGER.info("Server starting on {}", masterSocketInfo);

    final ImmutableSet<Configuration> workerConfs = getWorkerConfs(injector);
    final ImmutableMap.Builder<Integer, SocketInfo> workersBuilder = ImmutableMap.builder();
    for (Configuration workerConf : workerConfs) {
      workersBuilder.put(
          getIdFromWorkerConf(workerConf),
          new SocketInfo(getHostFromWorkerConf(workerConf), getPortFromWorkerConf(workerConf)));
    }
    workers = workersBuilder.build();
    // aliveWorkers.addAll(workers.keySet());
    workerAddRemoveLock = Striped.lock(workers.size());

    final Map<Integer, SocketInfo> computingUnits = new HashMap<>(workers);
    computingUnits.put(MyriaConstants.MASTER_ID, masterSocketInfo);

    try {
      LOGGER.info("Attempting to open master catalog file under {}...", catalogPath);
      catalog = MasterCatalog.open(catalogPath);
    } catch (FileNotFoundException e) {
      LOGGER.info(
          "Failed to open master catalog file under {}, attempting to create it...\n({})",
          catalogPath,
          e.getMessage());
      catalog = MasterCatalog.create(catalogPath);
    }
    queryManager = new QueryManager(catalog, this);

    connectionPool =
        new IPCConnectionPool(
            MyriaConstants.MASTER_ID,
            computingUnits,
            IPCConfigurations.createMasterIPCServerBootstrap(
                connectTimeoutMillis,
                sendBufferSize,
                receiveBufferSize,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark),
            IPCConfigurations.createMasterIPCClientBootstrap(
                connectTimeoutMillis,
                sendBufferSize,
                receiveBufferSize,
                writeBufferLowWaterMark,
                writeBufferHighWaterMark),
            new TransportMessageSerializer(),
            new QueueBasedShortMessageProcessor<TransportMessage>(messageQueue),
            inputBufferCapacity,
            inputBufferRecoverTrigger);

    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new RenamingThreadFactory("Master global timer"));
    scheduledTaskExecutor.scheduleAtFixedRate(
        new DebugHelper(), DebugHelper.INTERVAL, DebugHelper.INTERVAL, TimeUnit.MILLISECONDS);
    messageProcessingExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Master message processor"));
    serverQueryExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Master query executor"));

    /** The {@link Executor} who deals with IPC connection setup/cleanup. */
    ExecutorService ipcBossExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC boss"));
    /** The {@link Executor} who deals with IPC message delivering and transformation. */
    ExecutorService ipcWorkerExecutor =
        Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC worker"));

    ipcPipelineExecutor = null; // Remove the pipeline executor.
    // new
    // OrderedMemoryAwareThreadPoolExecutor(Runtime.getRuntime().availableProcessors()
    // * 2 + 1,
    // 5 * MyriaConstants.MB, 0,
    // MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS,
    // TimeUnit.MILLISECONDS,
    // new RenamingThreadFactory("Master Pipeline executor"));

    /** The {@link ChannelFactory} for creating client side connections. */
    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(
            ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime().availableProcessors() * 2 + 1);

    /** The {@link ChannelFactory} for creating server side accepted connections. */
    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(
            ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime().availableProcessors() * 2 + 1);
    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.

    ChannelPipelineFactory serverPipelineFactory =
        new IPCPipelineFactories.MasterServerPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory clientPipelineFactory =
        new IPCPipelineFactories.MasterClientPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory masterInJVMPipelineFactory =
        new IPCPipelineFactories.MasterInJVMPipelineFactory(connectionPool);

    connectionPool.start(
        serverChannelFactory,
        serverPipelineFactory,
        clientChannelFactory,
        clientPipelineFactory,
        masterInJVMPipelineFactory,
        new InJVMLoopbackChannelSink());

    messageProcessingExecutor.submit(new MessageProcessor());
    LOGGER.info("Server started on {}", masterSocketInfo);

    if (getDBMS().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
      final List<Integer> workerIds = ImmutableList.copyOf(workers.keySet());
      addRelationToCatalog(
          MyriaConstants.EVENT_PROFILING_RELATION,
          MyriaConstants.EVENT_PROFILING_SCHEMA,
          workerIds,
          false);
      addRelationToCatalog(
          MyriaConstants.SENT_PROFILING_RELATION,
          MyriaConstants.SENT_PROFILING_SCHEMA,
          workerIds,
          false);
      addRelationToCatalog(
          MyriaConstants.RESOURCE_PROFILING_RELATION,
          MyriaConstants.RESOURCE_PROFILING_SCHEMA,
          workerIds,
          false);
      addRelationToCatalog(
          MyriaConstants.PYUDF_RELATION, MyriaConstants.PYUDF_SCHEMA, workerIds, false);
    }
    perfEnforceDriver = new PerfEnforceDriver(this, instancePath);
  }

  /**
   * Manually add a relation to the catalog.
   *
   * @param relationKey the relation to add
   * @param schema the schema of the relation to add
   * @param workers the workers that have the relation
   * @param force force add the relation; will replace an existing entry.
   * @throws DbException if the catalog cannot be accessed
   */
  private void addRelationToCatalog(
      final RelationKey relationKey,
      final Schema schema,
      final List<Integer> workers,
      final boolean force)
      throws DbException {
    try {
      if (!force && getSchema(relationKey) != null) {
        return;
      }

      QueryEncoding query = new QueryEncoding();
      query.rawQuery = String.format("Add %s to catalog", relationKey);
      query.logicalRa = query.rawQuery;
      query.fragments = ImmutableList.of();

      long queryId = catalog.newQuery(query);

      final Query queryState =
          new Query(
              queryId,
              query,
              new SubQuery(
                  new SubQueryPlan(new EmptySink(new EOSSource())),
                  new HashMap<Integer, SubQueryPlan>()),
              this);
      queryState.markSuccess();
      catalog.queryFinished(queryState);

      Map<RelationKey, RelationWriteMetadata> relation = new HashMap<>();
      RelationWriteMetadata meta = new RelationWriteMetadata(relationKey, schema, true, false);
      for (Integer worker : workers) {
        meta.addWorker(worker);
      }
      relation.put(relationKey, meta);

      catalog.updateRelationMetadata(relation, new SubQueryId(queryId, 0));
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /** @return the dbms from {@link #execEnvVars}. */
  public String getDBMS() {
    return (String) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM);
  }

  /**
   * Can be only used in test.
   *
   * @return true if the query plan is accepted and scheduled for execution.
   * @param masterRoot the root operator of the master plan
   * @param workerRoots the roots of the worker part of the plan, {workerID -> RootOperator[]}
   * @throws DbException if any error occurs.
   * @throws CatalogException catalog errors.
   */
  public QueryFuture submitQueryPlan(
      final RootOperator masterRoot, final Map<Integer, RootOperator[]> workerRoots)
      throws DbException, CatalogException {
    String catalogInfoPlaceHolder = "MasterPlan: " + masterRoot + "; WorkerPlan: " + workerRoots;
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (Entry<Integer, RootOperator[]> entry : workerRoots.entrySet()) {
      workerPlans.put(entry.getKey(), new SubQueryPlan(entry.getValue()));
    }
    return queryManager.submitQuery(
        catalogInfoPlaceHolder,
        catalogInfoPlaceHolder,
        catalogInfoPlaceHolder,
        new SubQueryPlan(masterRoot),
        workerPlans);
  }

  /** @return the set of workers that are currently alive. */
  public Set<Integer> getAliveWorkers() {
    return ImmutableSet.copyOf(aliveWorkers);
  }

  /**
   * Return a random subset of workers.
   *
   * @param number the number of alive workers returned
   * @return a subset of workers that are currently alive.
   */
  public Set<Integer> getRandomWorkers(final int number) {
    Preconditions.checkArgument(
        number <= getAliveWorkers().size(),
        "The number of workers requested cannot exceed the number of alive workers.");
    if (number == getAliveWorkers().size()) {
      return getAliveWorkers();
    }
    List<Integer> workerList = new ArrayList<>(getAliveWorkers());
    Collections.shuffle(workerList);
    return ImmutableSet.copyOf(workerList.subList(0, number));
  }

  /** @return the set of known workers in this Master. */
  public Map<Integer, SocketInfo> getWorkers() {
    return workers;
  }

  /**
   * Ingest the given dataset.
   *
   * @param relationKey the name of the dataset.
   * @param workersToIngest restrict the workers to ingest data (null for all)
   * @param indexes the indexes created.
   * @param source the source of tuples to be ingested.
   * @param df the distribute function.
   * @return the status of the ingested dataset.
   * @throws InterruptedException interrupted
   * @throws DbException if there is an error
   */
  public DatasetStatus ingestDataset(
      final RelationKey relationKey,
      List<Integer> workersToIngest,
      final List<List<IndexRef>> indexes,
      final Operator source,
      final DistributeFunction df)
      throws InterruptedException, DbException {
    /* Figure out the workers we will use. If workersToIngest is null, use all active workers. */
    if (workersToIngest == null) {
      workersToIngest = ImmutableList.copyOf(getAliveWorkers());
    }
    int[] workersArray = Ints.toArray(workersToIngest);
    Preconditions.checkArgument(workersArray.length > 0, "Must use > 0 workers");

    /* The master plan: send the tuples out. */
    ExchangePairID scatterId = ExchangePairID.newID();
    df.setDestinations(workersArray.length, 1);
    GenericShuffleProducer scatter =
        new GenericShuffleProducer(source, new ExchangePairID[] {scatterId}, workersArray, df);

    /* The workers' plan */
    Consumer gather =
        new Consumer(source.getSchema(), scatterId, ImmutableSet.of(MyriaConstants.MASTER_ID));
    DbInsert insert = new DbInsert(gather, relationKey, true, indexes);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (Integer workerId : workersArray) {
      workerPlans.put(workerId, new SubQueryPlan(insert));
    }

    ListenableFuture<Query> qf;
    try {
      qf =
          queryManager.submitQuery(
              "ingest " + relationKey.toString(),
              "ingest " + relationKey.toString(),
              "ingest " + relationKey.toString(getDBMS()),
              new SubQueryPlan(scatter),
              workerPlans);
    } catch (CatalogException e) {
      throw new DbException("Error submitting query", e);
    }
    try {
      qf.get();
    } catch (ExecutionException e) {
      throw new DbException("Error executing query", e.getCause());
    }

    // updating the partition function only after it's successfully ingested.
    updateHowDistributed(relationKey, new HowDistributed(df, workersArray));
    return getDatasetStatus(relationKey);
  }

  /**
   * Parallel Ingest
   *
   * @param relationKey the name of the dataset.
   * @param workersToIngest restrict the workers to ingest data (null for all)
   * @throws URIException
   * @throws DbException
   * @throws InterruptedException
   */
  public DatasetStatus parallelIngestDataset(
      final RelationKey relationKey,
      final Schema schema,
      @Nullable final Character delimiter,
      @Nullable final Character quote,
      @Nullable final Character escape,
      @Nullable final Integer numberOfSkippedLines,
      final AmazonS3Source s3Source,
      final Set<Integer> workersToIngest,
      final DistributeFunction distributeFunction)
      throws URIException, DbException, InterruptedException {
    long fileSize = s3Source.getFileSize();

    Set<Integer> potentialWorkers = MoreObjects.firstNonNull(workersToIngest, getAliveWorkers());

    /* Select a subset of workers */
    int[] workersArray = parallelIngestComputeNumWorkers(fileSize, potentialWorkers);

    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (int workerID = 1; workerID <= workersArray.length; workerID++) {
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(
              s3Source, schema, workersArray, delimiter, quote, escape, numberOfSkippedLines);
      workerPlans.put(
          workersArray[workerID - 1],
          new SubQueryPlan(new DbInsert(scanFragment, relationKey, true)));
    }

    ListenableFuture<Query> qf;
    try {
      qf =
          queryManager.submitQuery(
              "ingest " + relationKey.toString(),
              "ingest " + relationKey.toString(),
              "ingest " + relationKey.toString(getDBMS()),
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
    } catch (CatalogException e) {
      throw new DbException("Error submitting query", e);
    }

    try {
      qf.get();
    } catch (ExecutionException e) {
      throw new DbException("Error executing query", e.getCause());
    }

    updateHowDistributed(relationKey, new HowDistributed(distributeFunction, workersArray));
    return getDatasetStatus(relationKey);
  }

  /**
   * Helper method for parallel ingest.
   *
   * @param fileSize the size of the file to ingest
   * @param allWorkers all workers considered for ingest
   */
  public int[] parallelIngestComputeNumWorkers(long fileSize, Set<Integer> allWorkers) {
    /* Determine the number of workers to ingest based on partition size */
    int totalNumberOfWorkersToIngest = 0;
    for (int i = allWorkers.size(); i >= 1; i--) {
      totalNumberOfWorkersToIngest = i;
      long currentPartitionSize = fileSize / i;
      if (currentPartitionSize > MyriaConstants.PARALLEL_INGEST_WORKER_MINIMUM_PARTITION_SIZE) {
        break;
      }
    }
    int[] workersArray = new int[allWorkers.size()];
    int wCounter = 0;
    for (Integer w : allWorkers) {
      workersArray[wCounter] = w;
      wCounter++;
    }
    Arrays.sort(workersArray);
    workersArray = Arrays.copyOfRange(workersArray, 0, totalNumberOfWorkersToIngest);
    return workersArray;
  }

  /**
   * @param relationKey the relationalKey of the dataset to import
   * @param schema the schema of the dataset to import
   * @param workersToImportFrom the set of workers
   * @throws DbException if there is an error
   * @throws InterruptedException interrupted
   */
  public void addDatasetToCatalog(
      final RelationKey relationKey, final Schema schema, final List<Integer> workersToImportFrom)
      throws DbException, InterruptedException {

    /* Figure out the workers we will use. If workersToImportFrom is null, use all active workers. */
    List<Integer> actualWorkers = workersToImportFrom;
    if (workersToImportFrom == null) {
      actualWorkers = ImmutableList.copyOf(getWorkers().keySet());
    }
    addRelationToCatalog(relationKey, schema, workersToImportFrom, true);

    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : actualWorkers) {
        workerPlans.put(
            workerId, new SubQueryPlan(new DbInsert(EmptyRelation.of(schema), relationKey, false)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "add to catalog " + relationKey.toString(),
              "add to catalog " + relationKey.toString(),
              "add to catalog " + relationKey.toString(getDBMS()),
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        qf.get();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param relationKey the relationKey of the dataset to delete
   * @return the status
   * @throws DbException if there is an error
   * @throws InterruptedException interrupted
   */
  public void deleteDataset(final RelationKey relationKey)
      throws DbException, InterruptedException {

    /* Mark the relation as is_deleted */
    try {
      catalog.markRelationDeleted(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* Delete from postgres at each worker by calling the DbDelete operator */
    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : getWorkersForRelation(relationKey)) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbDelete(EmptyRelation.of(catalog.getSchema(relationKey)), relationKey, null)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "delete " + relationKey.toString(),
              "delete " + relationKey.toString(),
              "deleting from " + relationKey.toString(getDBMS()),
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        qf.get();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* Deleting from the catalog */
    try {
      catalog.deleteRelationFromCatalog(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /** Create indexes and add the metadata to the catalog */
  public long addIndexesToRelation(
      final RelationKey relationKey, final Schema schema, final List<IndexRef> indexes)
      throws DbException, InterruptedException {
    long queryID;
    /* Add indexes to relations */
    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : getWorkersForRelation(relationKey)) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbCreateIndex(
                    EmptyRelation.of(catalog.getSchema(relationKey)),
                    relationKey,
                    schema,
                    indexes,
                    null)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "add indexes to " + relationKey.toString(),
              "add indexes to  " + relationKey.toString(),
              "add indexes to " + relationKey.toString(getDBMS()),
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        queryID = qf.get().getQueryId();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* Add index to catalog */
    try {
      catalog.markIndexesInCatalog(relationKey, indexes);
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    return queryID;
  }

  /** Create a view */
  public long createView(
      final String viewName, final String viewDefinition, final Set<Integer> workers)
      throws DbException, InterruptedException {
    long queryID;
    Set<Integer> actualWorkers = workers;
    if (workers == null) {
      actualWorkers = getWorkers().keySet();
    }

    /* Create the view */
    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : actualWorkers) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbCreateView(
                    EmptyRelation.of(Schema.EMPTY_SCHEMA), viewName, viewDefinition, false, null)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "create view",
              "create view",
              "create view",
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        queryID = qf.get().getQueryId();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    return queryID;
  }

  /**
   * Create a materialized view
   * @param viewName the name of the view
   * @param viewDefinition the sql text for the view
   * @param workers the workers creating the view
   * @return the queryID for the view creation query
   */
  public long createMaterializedView(
      final String viewName, final String viewDefinition, final Set<Integer> workers)
      throws DbException, InterruptedException {
    long queryID;
    Set<Integer> actualWorkers = workers;
    if (workers == null) {
      actualWorkers = getWorkers().keySet();
    }

    /* Create the view */
    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : actualWorkers) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbCreateView(
                    EmptyRelation.of(Schema.EMPTY_SCHEMA), viewName, viewDefinition, true, null)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "create materialized view",
              "create materialized view",
              "create materialized view",
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        queryID = qf.get().getQueryId();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    return queryID;
  }

  /**
   * Create a function and register it in the catalog
   *
   * @param name the name of the function
   * @param definition the function definition - this is postgres specific for postgres and function text for python.
   * @param outputType the output schema of the function
   * @param isMultiValued indicates if the function returns multiple tuples.
   * @param lang this is the language of the function.
   * @param binary this is an optional parameter for function for base64 encoded binary for function.
   * @param workers list of workers on which the function is registered: default is all.
   * @return the status of the function
   */
  public long createFunction(
      final String name,
      final String shortName,
      final String definition,
      final String outputType,
      final Boolean isMultiValued,
      final FunctionLanguage lang,
      final String binary,
      final String binaryUri,
      final Set<Integer> workers)
      throws DbException, InterruptedException {
    long queryID = 0;

    Set<Integer> actualWorkers = workers;
    if (workers == null) {
      actualWorkers = getWorkers().keySet();
    }
    try {

      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : actualWorkers) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbCreateFunction(
                    EmptyRelation.of(Schema.EMPTY_SCHEMA),
                    name,
                    shortName,
                    definition,
                    outputType,
                    isMultiValued,
                    lang,
                    binary,
                    binaryUri)));
      }

      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "create function",
              "create function",
              "create function",
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);

      try {
        queryID = qf.get().getQueryId();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e);
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    /* Register the function to the catalog don't send the binary. */
    try {
      catalog.registerFunction(name, definition, outputType, isMultiValued, lang);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    return queryID;
  }

  /**
   * @return list of functions from the catalog
   * @throws DbException in case of error.
   */
  public List<String> getFunctions() throws DbException {
    try {
      return catalog.getFunctions();
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param functionName : name of the function to retrieve.
   * @return functiondetails for the function
   * @throws DbException in case of error.
   */
  public FunctionStatus getFunctionDetails(final String functionName) throws DbException {
    try {
      return catalog.getFunctionStatus(functionName);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param relationKey the relationKey of the dataset to persist
   * @return the queryID
   * @throws DbException if there is an error
   * @throws InterruptedException interrupted
   */
  public long persistDataset(final RelationKey relationKey)
      throws DbException, InterruptedException, URISyntaxException {
    long queryID;

    /* Mark the relation as is_persistent */
    try {
      catalog.markRelationPersistent(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* Create the query plan for persist */
    try {
      ImmutableMap.Builder<Integer, SubQueryPlan> workerPlans =
          new ImmutableMap.Builder<Integer, SubQueryPlan>();
      for (Integer workerId : getWorkersForRelation(relationKey)) {
        String partitionName =
            String.format(
                persistURI + "/myria-system/partition-%s/%s/%s/%s",
                workerId,
                relationKey.getUserName(),
                relationKey.getProgramName(),
                relationKey.getRelationName());
        DataSink workerSink = new UriSink(partitionName);
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new TupleSink(
                    new DbQueryScan(relationKey, getSchema(relationKey)),
                    new PostgresBinaryTupleWriter(),
                    workerSink)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "persist " + relationKey.toString(),
              "persist " + relationKey.toString(),
              "persisting from " + relationKey.toString(getDBMS()),
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans.build());
      try {
        queryID = qf.get().getQueryId();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    return queryID;
  }

  /**
   * Directly runs a command on the underlying database based on the selected workers
   *
   * @param sqlString command to run on the database
   * @param workers the workers that will run the command
   */
  public void executeSQLStatement(final String sqlString, final Set<Integer> workers)
      throws DbException, InterruptedException {

    /* Execute the SQL command on the set of workers */
    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : workers) {
        workerPlans.put(
            workerId,
            new SubQueryPlan(
                new DbExecute(EmptyRelation.of(Schema.EMPTY_SCHEMA), sqlString, null)));
      }
      ListenableFuture<Query> qf =
          queryManager.submitQuery(
              "sql execute " + sqlString,
              "sql execute " + sqlString,
              "sql execute " + sqlString,
              new SubQueryPlan(new EmptySink(new EOSSource())),
              workerPlans);
      try {
        qf.get();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Directly runs a command on the underlying database based on the selected workers
   * and returns the tuple results through a string array
   *
   * @param sqlString command to run on the database
   * @param outputSchema the schema of the output result
   * @param workers the workers that will run the command
   * @return the resulting tuples from the SQL statement
   */
  public String[] executeSQLStatement(
      final String sqlString, final Schema outputSchema, final Set<Integer> workers)
      throws DbException {

    ByteSink byteSink = new ByteSink();
    TupleWriter writer = new CsvTupleWriter();

    DbQueryScan scan = new DbQueryScan(sqlString, outputSchema);
    final ExchangePairID operatorId = ExchangePairID.newID();
    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);
    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (Integer w : workers) {
      workerPlans.put(w, workerPlan);
    }

    final Consumer consumer = new Consumer(outputSchema, operatorId, workers);
    TupleSink output = new TupleSink(consumer, writer, byteSink, false);

    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    String planString = "execute sql statement : " + sqlString;
    try {
      queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans).get();
    } catch (Exception e) {
      throw new DbException();
    }

    byte[] responseBytes;
    try {
      responseBytes = ((ByteArrayOutputStream) byteSink.getOutputStream()).toByteArray();
    } catch (IOException e) {
      throw new DbException();
    }
    String response = new String(responseBytes, Charset.forName("UTF-8"));
    String[] tuples = response.split("\r\n");

    return tuples;
  }

  /**
   * @param relationKey the key of the desired relation.
   * @return the schema of the specified relation, or null if not found.
   * @throws CatalogException if there is an error getting the Schema out of the catalog.
   */
  public Schema getSchema(final RelationKey relationKey) throws CatalogException {
    if (relationKey.isTemp()) {
      return queryManager.getQuery(relationKey.tempRelationQueryId()).getTempSchema(relationKey);
    }
    return catalog.getSchema(relationKey);
  }

  /**
   * @param key the relation key.
   * @param howPartitioned how the dataset was partitioned.
   */
  public void updateHowDistributed(final RelationKey key, final HowDistributed howDistributed)
      throws DbException {
    try {
      catalog.updateHowDistributed(key, howDistributed);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param relationKey the key of the desired temp relation.
   * @throws CatalogException if there is an error accessing the catalog.
   * @return the set of workers that store the specified relation.
   */
  public @Nonnull Set<Integer> getWorkersForRelation(@Nonnull final RelationKey relationKey)
      throws CatalogException {
    if (relationKey.isTemp()) {
      return queryManager
          .getQuery(relationKey.tempRelationQueryId())
          .getWorkersForTempRelation(relationKey);
    } else {
      return catalog.getWorkersForRelationKey(relationKey);
    }
  }

  /** @return the socket info for the master. */
  protected SocketInfo getSocketInfo() {
    return masterSocketInfo;
  }

  /**
   * @return A list of datasets in the system.
   * @throws DbException if there is an error accessing the desired Schema.
   */
  public List<DatasetStatus> getDatasets() throws DbException {
    try {
      return catalog.getDatasets();
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Get the metadata about a relation.
   *
   * @param relationKey specified which relation to get the metadata about.
   * @return the metadata of the specified relation.
   * @throws DbException if there is an error getting the status.
   */
  public DatasetStatus getDatasetStatus(final RelationKey relationKey) throws DbException {
    try {
      return catalog.getDatasetStatus(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param searchTerm the search term
   * @return the relations that match the search term
   * @throws DbException if there is an error getting the relation keys.
   */
  public List<RelationKey> getMatchingRelationKeys(final String searchTerm) throws DbException {
    try {
      return catalog.getMatchingRelationKeys(searchTerm);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param userName the user whose datasets we want to access.
   * @return a list of datasets belonging to the specified user.
   * @throws DbException if there is an error accessing the Catalog.
   */
  public List<DatasetStatus> getDatasetsForUser(final String userName) throws DbException {
    try {
      return catalog.getDatasetsForUser(userName);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param userName the user whose datasets we want to access.
   * @param programName the program by that user whose datasets we want to access.
   * @return a list of datasets belonging to the specified program.
   * @throws DbException if there is an error accessing the Catalog.
   */
  public List<DatasetStatus> getDatasetsForProgram(final String userName, final String programName)
      throws DbException {
    try {
      return catalog.getDatasetsForProgram(userName, programName);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId the id of the query.
   * @return a list of datasets belonging to the specified program.
   * @throws DbException if there is an error accessing the Catalog.
   */
  public List<DatasetStatus> getDatasetsForQuery(final int queryId) throws DbException {
    try {
      return catalog.getDatasetsForQuery(queryId);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @return the maximum query id that matches the search.
   * @param searchTerm a token to match against the raw queries. If null, all queries match.
   * @throws CatalogException if an error occurs
   */
  public long getMaxQuery(final String searchTerm) throws CatalogException {
    return catalog.getMaxQuery(searchTerm);
  }

  /**
   * @return the minimum query id that matches the search.
   * @param searchTerm a token to match against the raw queries. If null, all queries match.
   * @throws CatalogException if an error occurs
   */
  public long getMinQuery(final String searchTerm) throws CatalogException {
    return catalog.getMinQuery(searchTerm);
  }

  /**
   * Start a query that streams tuples from the specified relation to the specified {@link TupleWriter}.
   *
   * @param relationKey the relation to be downloaded.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return the query future from which the query status can be looked up.
   * @throws DbException if there is an error in the system.
   */
  public ListenableFuture<Query> startDataStream(
      final RelationKey relationKey, final TupleWriter writer, final DataSink dataSink)
      throws DbException {
    /* Get the relation's schema, to make sure it exists. */
    final Schema schema;
    try {
      schema = catalog.getSchema(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    Preconditions.checkArgument(schema != null, "relation %s was not found", relationKey);

    /* Get the workers that store it. */
    Set<Integer> scanWorkers;
    try {
      scanWorkers = getWorkersForRelation(relationKey);
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    /* If relation is broadcast, pick random worker to scan. */
    DistributeFunction df = getDatasetStatus(relationKey).getHowDistributed().getDf();
    if (df instanceof BroadcastDistributeFunction) {
      scanWorkers = ImmutableSet.of(scanWorkers.iterator().next());
    }

    /* Construct the operators that go elsewhere. */
    DbQueryScan scan = new DbQueryScan(relationKey, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();
    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(scanWorkers.size());
    for (Integer worker : scanWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Construct the master plan. */
    final Consumer consumer = new Consumer(schema, operatorId, ImmutableSet.copyOf(scanWorkers));
    TupleSink output = new TupleSink(consumer, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString = "download " + relationKey.toString();
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Start a query that streams tuples from the specified relation to the specified {@link TupleWriter}.
   *
   * @param numTB the number of {@link TupleBatch}es to download from each worker.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return the query future from which the query status can be looked up.
   * @throws DbException if there is an error in the system.
   */
  public ListenableFuture<Query> startTestDataStream(
      final int numTB, final TupleWriter writer, final DataSink dataSink) throws DbException {

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    Random r = new Random();
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < tbb.getBatchSize(); i++) {
      tbb.putLong(0, r.nextLong());
      tbb.putString(1, new java.util.Date().toString());
    }

    TupleBatch tb = tbb.popAny();

    final DuplicateTBGenerator scanTable = new DuplicateTBGenerator(tb, numTB);

    /* Get the workers that store it. */
    Set<Integer> scanWorkers = getAliveWorkers();

    /* Construct the operators that go elsewhere. */
    final ExchangePairID operatorId = ExchangePairID.newID();
    CollectProducer producer = new CollectProducer(scanTable, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(scanWorkers.size());
    for (Integer worker : scanWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Construct the master plan. */
    final Consumer consumer = new Consumer(schema, operatorId, ImmutableSet.copyOf(scanWorkers));
    TupleSink output = new TupleSink(consumer, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString = "download test";
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param subqueryId the subquery id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param writer writer to get data.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public ListenableFuture<Query> startSentLogDataStream(
      final SubQueryId subqueryId,
      final long fragmentId,
      final TupleWriter writer,
      final DataSink dataSink)
      throws DbException {
    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    String fragmentWhere = "";
    if (fragmentId >= 0) {
      fragmentWhere = "AND \"fragmentId\" = " + fragmentId;
    }

    final Schema schema =
        Schema.ofFields(
            "fragmentId", Type.INT_TYPE, "destWorker", Type.INT_TYPE, "numTuples", Type.LONG_TYPE);

    String sentQueryString =
        Joiner.on(' ')
            .join(
                "SELECT \"fragmentId\", \"destWorkerId\", sum(\"numTuples\") as \"numTuples\" FROM",
                MyriaConstants.SENT_PROFILING_RELATION.toString(getDBMS()),
                "WHERE \"queryId\" =",
                subqueryId.getQueryId(),
                "AND \"subQueryId\" =",
                subqueryId.getSubqueryId(),
                fragmentWhere,
                "GROUP BY \"fragmentId\", \"destWorkerId\"");

    DbQueryScan scan = new DbQueryScan(sentQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    ImmutableList.Builder<Expression> emitExpressions = ImmutableList.builder();

    emitExpressions.add(new Expression("workerId", new WorkerIdExpression()));

    for (int column = 0; column < schema.numColumns(); column++) {
      VariableExpression copy = new VariableExpression(column);
      emitExpressions.add(new Expression(schema.getColumnName(column), copy));
    }

    Apply addWorkerId = new Apply(scan, emitExpressions.build());

    CollectProducer producer =
        new CollectProducer(addWorkerId, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    final Consumer consumer =
        new Consumer(addWorkerId.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    final Aggregate aggregate =
        new Aggregate(
            consumer, new int[] {0, 1, 2}, new PrimitiveAggregatorFactory(3, AggregationOp.SUM));

    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("src", new VariableExpression(0)));
    renameExpressions.add(new Expression("fragmentId", new VariableExpression(1)));
    renameExpressions.add(new Expression("dest", new VariableExpression(2)));
    renameExpressions.add(new Expression("numTuples", new VariableExpression(3)));
    final Apply rename = new Apply(aggregate, renameExpressions.build());

    TupleSink output = new TupleSink(rename, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download profiling sent data for (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ", fragment=",
                fragmentId,
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Extracts the set of workers used in a saved, encoded physical plan.
   *
   * @param plan a {@link List<PlanFragmentEncoding>}, cached during execution.
   * @return the set of workers used during the execution of this subquery.
   */
  @Nonnull
  private Set<Integer> getWorkersFromSubqueryPlan(final String plan) {
    /* We need to accumulate the workers used in the plan. We could deserialize the plan as a
     * List<PlanFragmentEncoding>... which it is, but for forwards and backwards compatiblity let's deserialize it as a
     * List<Map<String,Object>>... which it also is. */
    ObjectMapper mapper = MyriaJsonMapperProvider.getMapper();
    List<Map<String, Object>> fragments;
    Set<Integer> actualWorkers = Sets.newHashSet();
    try {
      fragments = mapper.readValue(plan, new TypeReference<List<Map<String, Object>>>() {});
      int fragIdx = 0;
      for (Map<String, Object> m : fragments) {
        Object fragWorkers = m.get("workers");
        Preconditions.checkNotNull(fragWorkers, "No workers recorded for fragment %s", fragIdx);
        Preconditions.checkState(
            fragWorkers instanceof Collection<?>,
            "Expected fragWorkers to be a collection, instead found %s",
            fragWorkers.getClass());
        try {
          @SuppressWarnings("unchecked")
          Collection<Integer> curWorkers = (Collection<Integer>) fragWorkers;
          actualWorkers.addAll(curWorkers);
        } catch (ClassCastException e) {
          throw new IllegalStateException(
              "Expected fragWorkers to be a collection of ints, instead found " + fragWorkers);
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Error deserializing workers from encoded plan " + plan, e);
    }
    /* Remove the MASTER from the set. */
    actualWorkers.remove(MyriaConstants.MASTER_ID);
    return actualWorkers;
  }

  /**
   * Returns the set of workers that executed a particular subquery.
   *
   * @param subQueryId the subquery.
   * @return the set of workers that executed a particular subquery.
   * @throws DbException if there is an error in the catalog.
   */
  private Set<Integer> getWorkersForSubQuery(final SubQueryId subQueryId) throws DbException {
    String serializedPlan;
    try {
      serializedPlan = catalog.getQueryPlan(subQueryId);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    Preconditions.checkArgument(
        serializedPlan != null, "No cached query plan for subquery %s", subQueryId);
    return getWorkersFromSubqueryPlan(serializedPlan);
  }

  /**
   * @param subqueryId the subquery id.
   * @param writer writer to get data.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public ListenableFuture<Query> startAggregatedSentLogDataStream(
      final SubQueryId subqueryId, final TupleWriter writer, final DataSink dataSink)
      throws DbException {
    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    final Schema schema =
        Schema.ofFields(
            "fragmentId",
            Type.INT_TYPE,
            "numTuples",
            Type.LONG_TYPE,
            "minTime",
            Type.LONG_TYPE,
            "maxTime",
            Type.LONG_TYPE);

    String sentQueryString =
        Joiner.on(' ')
            .join(
                "SELECT \"fragmentId\", sum(\"numTuples\") as \"numTuples\", min(\"nanoTime\") as \"minTime\", max(\"nanoTime\") as \"maxTime\" FROM",
                MyriaConstants.SENT_PROFILING_RELATION.toString(getDBMS()),
                "WHERE \"queryId\" =",
                subqueryId.getQueryId(),
                "AND \"subQueryId\" =",
                subqueryId.getSubqueryId(),
                "GROUP BY \"fragmentId\"");

    DbQueryScan scan = new DbQueryScan(sentQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    final Consumer consumer =
        new Consumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    final Aggregate aggregate =
        new Aggregate(
            consumer,
            new int[] {0},
            new PrimitiveAggregatorFactory(1, AggregationOp.SUM),
            new PrimitiveAggregatorFactory(2, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(3, AggregationOp.MAX));

    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("fragmentId", new VariableExpression(0)));
    renameExpressions.add(new Expression("numTuples", new VariableExpression(1)));
    renameExpressions.add(
        new Expression(
            "duration", new MinusExpression(new VariableExpression(3), new VariableExpression(2))));
    final Apply rename = new Apply(aggregate, renameExpressions.build());

    TupleSink output = new TupleSink(rename, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download profiling aggregated sent data for (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param subqueryId the desired subquery.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param start the earliest time where we need data
   * @param end the latest time
   * @param minSpanLength minimum length of a span to be returned
   * @param onlyRootOperator only return data for root operator
   * @param writer writer to get data.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startLogDataStream(
      final SubQueryId subqueryId,
      final long fragmentId,
      final long start,
      final long end,
      final long minSpanLength,
      final boolean onlyRootOperator,
      final TupleWriter writer,
      final DataSink dataSink)
      throws DbException {
    Preconditions.checkArgument(start < end, "range cannot be negative");

    final Schema schema =
        Schema.ofFields(
            "opId",
            Type.INT_TYPE,
            "startTime",
            Type.LONG_TYPE,
            "endTime",
            Type.LONG_TYPE,
            "numTuples",
            Type.LONG_TYPE);

    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    String opCondition = "";
    if (onlyRootOperator) {
      opCondition =
          Joiner.on(' ')
              .join(
                  "AND \"opId\" = (SELECT \"opId\" FROM",
                  MyriaConstants.EVENT_PROFILING_RELATION.toString(getDBMS()),
                  "WHERE \"fragmentId\" =",
                  fragmentId,
                  " AND \"queryId\"=",
                  subqueryId.getQueryId(),
                  "AND \"subQueryId\" =",
                  subqueryId.getSubqueryId(),
                  "ORDER BY \"startTime\" ASC LIMIT 1)");
    }

    String spanCondition = "";
    if (minSpanLength > 0) {
      spanCondition = Joiner.on(' ').join("AND \"endTime\" - \"startTime\" >", minSpanLength);
    }

    String queryString =
        Joiner.on(' ')
            .join(
                "SELECT \"opId\", \"startTime\", \"endTime\", \"numTuples\" FROM",
                MyriaConstants.EVENT_PROFILING_RELATION.toString(getDBMS()),
                "WHERE \"fragmentId\" =",
                fragmentId,
                "AND \"queryId\" =",
                subqueryId.getQueryId(),
                "AND \"subQueryId\" =",
                subqueryId.getSubqueryId(),
                "AND \"endTime\" >",
                start,
                "AND \"startTime\" <",
                end,
                opCondition,
                spanCondition,
                "ORDER BY \"startTime\" ASC");

    DbQueryScan scan = new DbQueryScan(queryString, schema);

    ImmutableList.Builder<Expression> emitExpressions = ImmutableList.builder();

    emitExpressions.add(new Expression("workerId", new WorkerIdExpression()));

    for (int column = 0; column < schema.numColumns(); column++) {
      VariableExpression copy = new VariableExpression(column);
      emitExpressions.add(new Expression(schema.getColumnName(column), copy));
    }

    Apply addWorkerId = new Apply(scan, emitExpressions.build());

    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer =
        new CollectProducer(addWorkerId, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    final Consumer consumer =
        new Consumer(addWorkerId.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    TupleSink output = new TupleSink(consumer, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download profiling data (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ", fragment=",
                fragmentId,
                ", range=[",
                Joiner.on(", ").join(start, end),
                "]",
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /** Upper bound on the number of bins a profiler can ask for. */
  private static final long MAX_BINS = 10000;

  /**
   * @param subqueryId subquery id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param start start of the histogram
   * @param end the end of the histogram
   * @param step the step size between min and max
   * @param onlyRootOp return histogram only for root operator
   * @param writer writer to get data.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startHistogramDataStream(
      final SubQueryId subqueryId,
      final long fragmentId,
      final long start,
      final long end,
      final long step,
      final boolean onlyRootOp,
      final TupleWriter writer,
      final DataSink dataSink)
      throws DbException {

    Preconditions.checkArgument(start < end, "range cannot be negative");
    Preconditions.checkArgument(step > 0, "step has to be greater than 0");
    long bins = (end - start + 1) / step;
    Preconditions.checkArgument(
        bins > 0 && bins <= MAX_BINS, "bins must be in the range [1, %s]", MAX_BINS);

    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    final Schema schema = Schema.ofFields("opId", Type.INT_TYPE, "nanoTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.EVENT_PROFILING_RELATION;

    Map<String, Object> queryArgs = new HashMap<>();
    queryArgs.put("QUERY", subqueryId.getQueryId());
    queryArgs.put("SUBQUERY", subqueryId.getSubqueryId());
    queryArgs.put("FRAGMENT", fragmentId);
    queryArgs.put("START", start);
    queryArgs.put("END", end);
    queryArgs.put("STEP", step);
    queryArgs.put("BINS", bins);
    queryArgs.put("PROF_TABLE", relationKey.toString(getDBMS()));
    StrSubstitutor sub;

    String filterOpnameQueryString = "";
    if (onlyRootOp) {
      sub = new StrSubstitutor(queryArgs);
      filterOpnameQueryString =
          sub.replace(
              "AND p.\"opId\"=(SELECT \"opId\" FROM ${PROF_TABLE} WHERE \"fragmentId\"=${FRAGMENT} AND \"queryId\"=${QUERY} AND \"subQueryId\"=${SUBQUERY} ORDER BY \"startTime\" ASC LIMIT 1)");
    }

    // Reinitialize the substitutor after including the opname filter.
    queryArgs.put("OPNAME_FILTER", filterOpnameQueryString);
    sub = new StrSubstitutor(queryArgs);

    String histogramWorkerQueryString =
        sub.replace(
            Joiner.on("\n")
                .join(
                    "SELECT \"opId\", ${START}::bigint+${STEP}::bigint*s.bin as \"nanoTime\"",
                    "FROM (",
                    "SELECT p.\"opId\", greatest((p.\"startTime\"-1-${START}::bigint)/${STEP}::bigint, -1) as \"startBin\", least((p.\"endTime\"+1-${START}::bigint)/${STEP}::bigint, ${BINS}) AS \"endBin\"",
                    "FROM ${PROF_TABLE} p",
                    "WHERE p.\"queryId\" = ${QUERY} and p.\"subQueryId\" = ${SUBQUERY} and p.\"fragmentId\" = ${FRAGMENT}",
                    "${OPNAME_FILTER}",
                    "AND greatest((p.\"startTime\"-${START}::bigint)/${STEP}::bigint, -1) < least((p.\"endTime\"-${START}::bigint)/${STEP}::bigint, ${BINS}) AND p.\"startTime\" < ${END}::bigint AND p.\"endTime\" >= ${START}::bigint",
                    ") times,",
                    "generate_series(0, ${BINS}) AS s(bin)",
                    "WHERE s.bin > times.\"startBin\" and s.bin <= times.\"endBin\";"));

    DbQueryScan scan = new DbQueryScan(histogramWorkerQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Aggregate histogram on master */
    final Consumer consumer =
        new Consumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // sum up the number of workers working
    final Aggregate sumAggregate =
        new Aggregate(
            consumer, new int[] {0, 1}, new PrimitiveAggregatorFactory(1, AggregationOp.COUNT));
    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("opId", new VariableExpression(0)));
    renameExpressions.add(new Expression("nanoTime", new VariableExpression(1)));
    renameExpressions.add(new Expression("numWorkers", new VariableExpression(2)));
    final Apply rename = new Apply(sumAggregate, renameExpressions.build());

    TupleSink output = new TupleSink(rename, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download profiling histogram (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ", fragment=",
                fragmentId,
                ", range=[",
                Joiner.on(", ").join(start, end, step),
                "]",
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param subqueryId the subquery id.
   * @param fragmentId the fragment id
   * @param writer writer to get data
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startRangeDataStream(
      final SubQueryId subqueryId,
      final long fragmentId,
      final TupleWriter writer,
      final DataSink dataSink)
      throws DbException {
    final Schema schema = Schema.ofFields("startTime", Type.LONG_TYPE, "endTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.EVENT_PROFILING_RELATION;

    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    String opnameQueryString =
        Joiner.on(' ')
            .join(
                "SELECT min(\"startTime\"), max(\"endTime\") FROM",
                relationKey.toString(getDBMS()),
                "WHERE \"queryId\"=",
                subqueryId.getQueryId(),
                "AND \"subQueryId\"=",
                subqueryId.getSubqueryId(),
                "AND \"fragmentId\"=",
                fragmentId);

    DbQueryScan scan = new DbQueryScan(opnameQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Construct the master plan. */
    final Consumer consumer =
        new Consumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // Aggregate range on master
    final Aggregate sumAggregate =
        new Aggregate(
            consumer,
            new int[] {},
            new PrimitiveAggregatorFactory(0, AggregationOp.MIN),
            new PrimitiveAggregatorFactory(1, AggregationOp.MAX));

    TupleSink output = new TupleSink(sumAggregate, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download time range (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ", fragment=",
                fragmentId,
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param subqueryId subquery id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param writer writer to get data.
   * @param dataSink the {@link DataSink} for the tuple destination
   * @return contributions for operator.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startContributionsStream(
      final SubQueryId subqueryId,
      final long fragmentId,
      final TupleWriter writer,
      final DataSink dataSink)
      throws DbException {
    final Schema schema = Schema.ofFields("opId", Type.INT_TYPE, "nanoTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.EVENT_PROFILING_RELATION;

    Set<Integer> actualWorkers = getWorkersForSubQuery(subqueryId);

    String fragIdCondition = "";
    if (fragmentId >= 0) {
      fragIdCondition = "AND \"fragmentId\"=" + fragmentId;
    }

    String opContributionsQueryString =
        Joiner.on(' ')
            .join(
                "SELECT \"opId\", sum(\"endTime\" - \"startTime\") FROM ",
                relationKey.toString(getDBMS()),
                "WHERE \"queryId\"=",
                subqueryId.getQueryId(),
                "AND \"subQueryId\"=",
                subqueryId.getSubqueryId(),
                fragIdCondition,
                "GROUP BY \"opId\"");

    DbQueryScan scan = new DbQueryScan(opContributionsQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Aggregate on master */
    final Consumer consumer =
        new Consumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // sum up contributions
    final Aggregate sumAggregate =
        new Aggregate(
            consumer, new int[] {0}, new PrimitiveAggregatorFactory(1, AggregationOp.AVG));

    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("opId", new VariableExpression(0)));
    renameExpressions.add(new Expression("nanoTime", new VariableExpression(1)));
    final Apply rename = new Apply(sumAggregate, renameExpressions.build());

    TupleSink output = new TupleSink(rename, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("")
            .join(
                "download operator contributions (query=",
                subqueryId.getQueryId(),
                ", subquery=",
                subqueryId.getSubqueryId(),
                ", fragment=",
                fragmentId,
                ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Update the {@link MasterCatalog} so that the specified relation has the specified tuple count.
   *
   * @param relation the relation to update
   * @param count the number of tuples in that relation
   * @throws DbException if there is an error in the catalog
   */
  public void updateRelationTupleCount(final RelationKey relation, final long count)
      throws DbException {
    try {
      catalog.updateRelationTupleCount(relation, count);
    } catch (CatalogException e) {
      throw new DbException("updating the number of tuples in the catalog", e);
    }
  }

  /**
   * Set the global variable owned by the specified query and named by the specified key to the specified value.
   *
   * @param queryId the query to whom the variable belongs.
   * @param key the name of the variable
   * @param value the new value for the variable
   */
  public void setQueryGlobal(
      final long queryId, @Nonnull final String key, @Nonnull final Object value) {
    Preconditions.checkNotNull(key, "key");
    Preconditions.checkNotNull(value, "value");
    queryManager.getQuery(queryId).setGlobal(key, value);
  }

  /**
   * Get the value of global variable owned by the specified query and named by the specified key.
   *
   * @param queryId the query to whom the variable belongs.
   * @param key the name of the variable
   * @return the value of the variable
   */
  @Nullable
  public Object getQueryGlobal(final long queryId, @Nonnull final String key) {
    Preconditions.checkNotNull(key, "key");
    return queryManager.getQuery(queryId).getGlobal(key);
  }

  /**
   * @param queryId the query id to fetch
   * @param writerOutput the output stream to write results to.
   * @throws DbException if there is an error in the database.
   */
  public void getResourceUsage(final long queryId, final DataSink dataSink) throws DbException {
    Schema schema =
        Schema.appendColumn(MyriaConstants.RESOURCE_PROFILING_SCHEMA, Type.INT_TYPE, "workerId");
    try {
      TupleWriter writer = new CsvTupleWriter();
      TupleBuffer tb = queryManager.getResourceUsage(queryId);
      if (tb != null) {
        writer.open(dataSink.getOutputStream());
        writer.writeColumnHeaders(schema.getColumnNames());
        writer.writeTuples(tb);
        writer.done();
        return;
      }
      getResourceLog(queryId, writer, dataSink);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId query id.
   * @param writer writer to get data.
   * @return resource logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public ListenableFuture<Query> getResourceLog(
      final long queryId, final TupleWriter writer, final DataSink dataSink) throws DbException {
    SubQueryId sqId = new SubQueryId(queryId, 0);
    String serializedPlan;
    try {
      serializedPlan = catalog.getQueryPlan(sqId);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
    Preconditions.checkArgument(
        serializedPlan != null, "No cached query plan for subquery %s", sqId);
    Set<Integer> actualWorkers = getWorkersFromSubqueryPlan(serializedPlan);

    final Schema schema = MyriaConstants.RESOURCE_PROFILING_SCHEMA;
    String resourceQueryString =
        Joiner.on(' ')
            .join(
                "SELECT * from",
                MyriaConstants.RESOURCE_PROFILING_RELATION.toString(getDBMS()),
                "WHERE \"queryId\" =",
                queryId);
    DbQueryScan scan = new DbQueryScan(resourceQueryString, schema);

    ImmutableList.Builder<Expression> emitExpressions = ImmutableList.builder();
    for (int column = 0; column < schema.numColumns(); column++) {
      VariableExpression copy = new VariableExpression(column);
      emitExpressions.add(new Expression(schema.getColumnName(column), copy));
    }
    emitExpressions.add(new Expression("workerId", new WorkerIdExpression()));
    Apply addWorkerId = new Apply(scan, emitExpressions.build());

    final ExchangePairID operatorId = ExchangePairID.newID();
    CollectProducer producer =
        new CollectProducer(addWorkerId, operatorId, MyriaConstants.MASTER_ID);
    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }
    final Consumer consumer =
        new Consumer(addWorkerId.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    TupleSink output = new TupleSink(consumer, writer, dataSink);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString = Joiner.on("").join("download resource log for (query=", queryId, ")");
    try {
      return queryManager.submitQuery(planString, planString, planString, masterPlan, workerPlans);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Record the fact that this subquery executed this in the catalog.
   *
   * @param subQueryId the id of the subquery.
   * @param encodedPlan the plan.
   * @throws DbException if there is an error in the catalog.
   */
  public void setQueryPlan(final SubQueryId subQueryId, @Nonnull final String encodedPlan)
      throws DbException {
    try {
      catalog.setQueryPlan(subQueryId, encodedPlan);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param subQueryId the query whose plan to look up.
   * @return the execution plan for this query.
   * @throws DbException if there is an error getting the query status.
   */
  @Nullable
  public String getQueryPlan(@Nonnull final SubQueryId subQueryId) throws DbException {
    try {
      return catalog.getQueryPlan(subQueryId);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /** @return the master catalog. */
  public MasterCatalog getCatalog() {
    return catalog;
  }

  /**
   * @return the perfenforce driver
   */
  public PerfEnforceDriver getPerfEnforceDriver() {
    return perfEnforceDriver;
  }
}
