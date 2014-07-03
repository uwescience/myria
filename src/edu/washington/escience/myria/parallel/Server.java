package edu.washington.escience.myria.parallel;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myria.parallel.ipc.QueueBasedShortMessageProcessor;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryMessage;
import edu.washington.escience.myria.proto.QueryProto.QueryReport;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.tool.MyriaConfigurationReader;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.DeploymentUtils;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.MyriaUtils;
import edu.washington.escience.myria.util.concurrent.ErrorLoggingTimerTask;
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;

/**
 * The master entrance.
 */
public final class Server {

  /**
   * Master message processor.
   */
  private final class MessageProcessor implements Runnable {

    /** Constructor, set the thread name. */
    public MessageProcessor() {
      super();
    }

    @Override
    public void run() {
      TERMINATE_MESSAGE_PROCESSING : while (true) {
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
              case WORKER_HEARTBEAT:
                LOGGER.trace("getting heartbeat from worker {}", senderID);
                updateHeartbeat(senderID);
                break;
              case REMOVE_WORKER_ACK:
                int workerID = controlM.getWorkerId();
                removeWorkerAckReceived.get(workerID).add(senderID);
                break;
              case ADD_WORKER_ACK:
                workerID = controlM.getWorkerId();
                addWorkerAckReceived.get(workerID).add(senderID);
                for (MasterSubQuery mqp : executingSubQueries.values()) {
                  if (mqp.getFTMode().equals(FTMODE.rejoin) && mqp.getMissingWorkers().contains(workerID)
                      && addWorkerAckReceived.get(workerID).containsAll(mqp.getWorkerAssigned())) {
                    /* so a following ADD_WORKER_ACK won't cause queryMessage to be sent again */
                    mqp.getMissingWorkers().remove(workerID);
                    try {
                      connectionPool.sendShortMessage(workerID, IPCUtils.queryMessage(mqp.getSubQueryId(), mqp
                          .getWorkerPlans().get(workerID)));
                    } catch (final IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
                }
                break;
              default:
                LOGGER.error("Unexpected control message received at master: {}", controlM);
            }
            break;
          case QUERY:
            final QueryMessage qm = m.getQueryMessage();
            final SubQueryId subQueryId = new SubQueryId(qm.getQueryId(), qm.getSubqueryId());
            MasterSubQuery mqp = executingSubQueries.get(subQueryId);
            switch (qm.getType()) {
              case QUERY_READY_TO_EXECUTE:
                mqp.queryReceivedByWorker(senderID);
                break;
              case QUERY_COMPLETE:
                QueryReport qr = qm.getQueryReport();
                if (qr.getSuccess()) {
                  mqp.workerComplete(senderID);
                } else {
                  ObjectInputStream osis = null;
                  Throwable cause = null;
                  try {
                    osis = new ObjectInputStream(new ByteArrayInputStream(qr.getCause().toByteArray()));
                    cause = (Throwable) (osis.readObject());
                  } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error("Error decoding failure cause", e);
                  }
                  mqp.workerFail(senderID, cause);
                  LOGGER.error("Worker #{} failed in executing query #{}.", senderID, subQueryId, cause);
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
      }
    }
  }

  /** The usage message for this server. */
  static final String USAGE = "Usage: Server catalogFile [-explain] [-f queryFile]";

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Server.class);

  /**
   * Initial worker list.
   */
  private final ConcurrentHashMap<Integer, SocketInfo> workers;

  /**
   * Queries currently active (queued, executing, being killed, etc.).
   */
  private final ConcurrentHashMap<Long, Query> activeQueries;

  /**
   * Subqueries currently in execution.
   */
  private final ConcurrentHashMap<SubQueryId, MasterSubQuery> executingSubQueries;

  /**
   * Current alive worker set.
   */
  private final ConcurrentHashMap<Integer, Long> aliveWorkers;

  /**
   * Scheduled new workers, when a scheduled worker sends the first heartbeat, it'll be removed from this set.
   */
  private final ConcurrentHashMap<Integer, SocketInfo> scheduledWorkers;

  /**
   * The time when new workers were scheduled.
   */
  private final ConcurrentHashMap<Integer, Long> scheduledWorkersTime;

  /**
   * Execution environment variables for operators.
   */
  private final ConcurrentHashMap<String, Object> execEnvVars;

  /**
   * All message queue.
   * 
   * @TODO remove this queue as in {@link Worker}s.
   */
  private final LinkedBlockingQueue<IPCMessage.Data<TransportMessage>> messageQueue;

  /**
   * The IPC Connection Pool.
   */
  private final IPCConnectionPool connectionPool;

  /**
   * {@link ExecutorService} for message processing.
   */
  private volatile ExecutorService messageProcessingExecutor;

  /** The Catalog stores the metadata about the Myria instance. */
  private final MasterCatalog catalog;

  /**
   * Default input buffer capacity for {@link Consumer} input buffers.
   */
  private final int inputBufferCapacity;

  /**
   * @return the system wide default inuput buffer recover event trigger.
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   */
  private final int inputBufferRecoverTrigger;

  /**
   * The {@link OrderedMemoryAwareThreadPoolExecutor} who gets messages from {@link workerExecutor} and further process
   * them using application specific message handlers, e.g. {@link MasterShortMessageProcessor}.
   */
  private volatile OrderedMemoryAwareThreadPoolExecutor ipcPipelineExecutor;

  /**
   * The {@link ExecutorService} who executes the master-side subqueries.
   */
  private volatile ExecutorService serverQueryExecutor;

  /**
   * @return the query executor used in this worker.
   */
  ExecutorService getQueryExecutor() {
    return serverQueryExecutor;
  }

  /**
   * max number of seconds for elegant cleanup.
   */
  public static final int NUM_SECONDS_FOR_ELEGANT_CLEANUP = 10;

  /** for each worker id, record the set of workers which REMOVE_WORKER_ACK have been received. */
  private final Map<Integer, Set<Integer>> removeWorkerAckReceived;
  /** for each worker id, record the set of workers which ADD_WORKER_ACK have been received. */
  private final Map<Integer, Set<Integer>> addWorkerAckReceived;

  /**
   * Entry point for the Master.
   * 
   * @param args the command line arguments.
   * @throws IOException if there's any error in reading catalog file.
   */
  public static void main(final String[] args) throws IOException {
    try {

      Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
      Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

      if (args.length < 1) {
        LOGGER.error(USAGE);
        System.exit(-1);
      }

      final String catalogName = args[0];

      final Server server = new Server(catalogName);

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Workers are: ");
        for (final Entry<Integer, SocketInfo> w : server.workers.entrySet()) {
          LOGGER.info(w.getKey() + ":  " + w.getValue().getHost() + ":" + w.getValue().getPort());
        }
      }

      Runtime.getRuntime().addShutdownHook(new Thread("Master shutdown hook cleaner") {
        @Override
        public void run() {
          final Thread cleaner = new Thread("Shutdown hook cleaner") {
            @Override
            public void run() {
              server.cleanup();
            }
          };

          final Thread countDown = new Thread("Shutdown hook countdown") {
            @Override
            public void run() {
              LOGGER.info("Wait for {} seconds for graceful cleaning-up.", Server.NUM_SECONDS_FOR_ELEGANT_CLEANUP);
              int i;
              for (i = Server.NUM_SECONDS_FOR_ELEGANT_CLEANUP; i > 0; i--) {
                try {
                  if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(i + "");
                  }
                  Thread.sleep(MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS);
                  if (!cleaner.isAlive()) {
                    break;
                  }
                } catch (InterruptedException e) {
                  break;
                }
              }
              if (i <= 0) {
                LOGGER.info("Graceful cleaning-up timeout. Going to shutdown abruptly.");
                for (Thread t : Thread.getAllStackTraces().keySet()) {
                  if (t != Thread.currentThread()) {
                    t.interrupt();
                  }
                }
              }
            }
          };
          cleaner.start();
          countDown.start();
          try {
            countDown.join();
          } catch (InterruptedException e) {
            // should not happen
            return;
          }
        }
      });
      server.start();
    } catch (Exception e) {
      LOGGER.error("Unknown error occurs at Master. Quit directly.", e);
    }
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
    return ipcPipelineExecutor;
  }

  /** The socket info for the master. */
  private final SocketInfo masterSocketInfo;

  /**
   * @return my execution environment variables for init of operators.
   */
  ConcurrentHashMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * @return execution mode.
   */
  QueryExecutionMode getExecutionMode() {
    return QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * Construct a server object, with configuration stored in the specified catalog file.
   * 
   * @param catalogFileName the name of the file containing the catalog.
   * @throws FileNotFoundException the specified file not found.
   * @throws CatalogException if there is an error reading from the Catalog.
   */
  public Server(final String catalogFileName) throws FileNotFoundException, CatalogException {
    catalog = MasterCatalog.open(catalogFileName);

    /* Get the master socket info */
    List<SocketInfo> masters = catalog.getMasters();
    if (masters.size() != 1) {
      throw new RuntimeException("Unexpected number of masters: expected 1, got " + masters.size());
    }
    masterSocketInfo = masters.get(MyriaConstants.MASTER_ID);

    workers = new ConcurrentHashMap<>(catalog.getWorkers());
    final ImmutableMap<String, String> allConfigurations = catalog.getAllConfigurations();

    inputBufferCapacity =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));

    inputBufferRecoverTrigger =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER));

    execEnvVars = new ConcurrentHashMap<>();
    for (Entry<String, String> cE : allConfigurations.entrySet()) {
      execEnvVars.put(cE.getKey(), cE.getValue());
    }
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_NODE_ID, MyriaConstants.MASTER_ID);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, getExecutionMode());

    aliveWorkers = new ConcurrentHashMap<>();
    scheduledWorkers = new ConcurrentHashMap<>();
    scheduledWorkersTime = new ConcurrentHashMap<>();

    removeWorkerAckReceived = new ConcurrentHashMap<>();
    addWorkerAckReceived = new ConcurrentHashMap<>();

    activeQueries = new ConcurrentHashMap<>();
    executingSubQueries = new ConcurrentHashMap<>();

    messageQueue = new LinkedBlockingQueue<>();

    final Map<Integer, SocketInfo> computingUnits = new HashMap<>(workers);
    computingUnits.put(MyriaConstants.MASTER_ID, masterSocketInfo);

    connectionPool =
        new IPCConnectionPool(MyriaConstants.MASTER_ID, computingUnits, IPCConfigurations
            .createMasterIPCServerBootstrap(this), IPCConfigurations.createMasterIPCClientBootstrap(this),
            new TransportMessageSerializer(), new QueueBasedShortMessageProcessor<TransportMessage>(messageQueue));

    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(new RenamingThreadFactory("Master global timer"));

    final String databaseSystem = catalog.getConfigurationValue(MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM);
    execEnvVars.put(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM, databaseSystem);
  }

  /**
   * timer task executor.
   */
  private final ScheduledExecutorService scheduledTaskExecutor;

  /**
   * This class presents only for the purpose of debugging. No other usage.
   */
  private class DebugHelper extends ErrorLoggingTimerTask {

    /**
     * Interval of execution.
     */
    public static final int INTERVAL = MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS;

    @Override
    public final synchronized void runInner() {
      System.currentTimeMillis();
    }
  }

  /**
   * The thread to check received REMOVE_WORKER_ACK and send ADD_WORKER to each worker. It's a temporary solution to
   * guarantee message synchronization. Once we have a generalized design in the IPC layer in the future, it can be
   * removed.
   */
  private Thread sendAddWorker = null;

  /**
   * The thread to check received REMOVE_WORKER_ACK and send ADD_WORKER to each worker.
   */
  private class SendAddWorker implements Runnable {

    /** the worker id that was removed. */
    private final int workerID;
    /** the expected number of REMOVE_WORKER_ACK messages to receive. */
    private int numOfAck;
    /** the socket info of the new worker. */
    private final SocketInfo socketInfo;

    /**
     * constructor.
     * 
     * @param workerID the removed worker id.
     * @param socketInfo the new worker's socket info.
     * @param numOfAck the number of REMOVE_WORKER_ACK to receive.
     */
    SendAddWorker(final int workerID, final SocketInfo socketInfo, final int numOfAck) {
      this.workerID = workerID;
      this.socketInfo = socketInfo;
      this.numOfAck = numOfAck;
    }

    @Override
    public void run() {
      while (numOfAck > 0) {
        for (int aliveWorkerId : aliveWorkers.keySet()) {
          if (removeWorkerAckReceived.get(workerID).remove(aliveWorkerId)) {
            numOfAck--;
            connectionPool.sendShortMessage(aliveWorkerId, IPCUtils.addWorkerTM(workerID, socketInfo));
          }
        }
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_100_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * @param workerID the worker to get updated
   */
  private void updateHeartbeat(final int workerID) {
    if (scheduledWorkers.containsKey(workerID)) {
      SocketInfo newWorker = scheduledWorkers.remove(workerID);
      scheduledWorkersTime.remove(workerID);
      if (newWorker != null) {
        sendAddWorker.start();
      }
    }
    aliveWorkers.put(workerID, System.currentTimeMillis());
  }

  /**
   * Check worker livenesses periodically. If a worker is detected as dead, its queries will be notified, it will be
   * removed from connection pools, and a new worker will be scheduled.
   */
  private class WorkerLivenessChecker extends ErrorLoggingTimerTask {

    @Override
    public final synchronized void runInner() {
      for (Integer workerId : aliveWorkers.keySet()) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - aliveWorkers.get(workerId) >= MyriaConstants.WORKER_IS_DEAD_INTERVAL) {
          /* scheduleAtFixedRate() is not accurate at all, use isRemoteAlive() to make sure the connection is lost. */
          if (connectionPool.isRemoteAlive(workerId)) {
            updateHeartbeat(workerId);
            continue;
          }

          LOGGER.info("worker {} doesn't have heartbeats, treat it as dead.", workerId);
          aliveWorkers.remove(workerId);

          for (MasterSubQuery mqp : executingSubQueries.values()) {
            /* for each alive query that the failed worker is assigned to, tell the query that the worker failed. */
            if (mqp.getWorkerAssigned().contains(workerId)) {
              mqp.workerFail(workerId, new LostHeartbeatException());
            }
            if (mqp.getFTMode().equals(FTMODE.abandon)) {
              mqp.getMissingWorkers().add(workerId);
              mqp.updateProducerChannels(workerId, false);
              mqp.triggerFragmentEosEoiChecks();
            } else if (mqp.getFTMode().equals(FTMODE.rejoin)) {
              mqp.getMissingWorkers().add(workerId);
              mqp.updateProducerChannels(workerId, false);
            }
          }

          removeWorkerAckReceived.put(workerId, Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
          addWorkerAckReceived.put(workerId, Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
          /* for using containsAll() later */
          addWorkerAckReceived.get(workerId).add(workerId);
          try {
            /* remove the failed worker from the connectionPool. */
            connectionPool.removeRemote(workerId).await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          /* tell other workers to remove it too. */
          for (int aliveWorkerId : aliveWorkers.keySet()) {
            connectionPool.sendShortMessage(aliveWorkerId, IPCUtils.removeWorkerTM(workerId));
          }

          /* Temporary solution: using exactly the same hostname:port. One good thing is the data is still there. */
          /* Temporary solution: using exactly the same worker id. */
          String newAddress = workers.get(workerId).getHost();
          int newPort = workers.get(workerId).getPort();
          int newWorkerId = workerId;

          /* a new worker will be launched, put its information in scheduledWorkers. */
          scheduledWorkers.put(newWorkerId, new SocketInfo(newAddress, newPort));
          scheduledWorkersTime.put(newWorkerId, currentTime);

          sendAddWorker =
              new Thread(new SendAddWorker(newWorkerId, new SocketInfo(newAddress, newPort), aliveWorkers.size()));
          connectionPool.putRemote(newWorkerId, new SocketInfo(newAddress, newPort));

          /* start a thread to launch the new worker. */
          new Thread(new NewWorkerScheduler(newWorkerId, newAddress, newPort)).start();
        }
      }
      for (Integer workerId : scheduledWorkers.keySet()) {
        long currentTime = System.currentTimeMillis();
        long time = scheduledWorkersTime.get(workerId);
        /* Had several trials and may need to change hostname:port of the scheduled new worker. */
        if (currentTime - time >= MyriaConstants.SCHEDULED_WORKER_UNABLE_TO_START) {
          SocketInfo si = scheduledWorkers.remove(workerId);
          scheduledWorkersTime.remove(workerId);
          connectionPool.removeRemote(workerId);
          LOGGER.error("Worker #{} ({}) failed to start. Give up.", workerId, si);
          continue;
          // Temporary solution: simply giving up launching this new worker
          // TODO: find a new set of hostname:port for this scheduled worker
        } else
        /* Haven't heard heartbeats from the scheduled worker, try to launch it again. */
        if (currentTime - time >= MyriaConstants.SCHEDULED_WORKER_FAILED_TO_START) {
          SocketInfo info = scheduledWorkers.get(workerId);
          new Thread(new NewWorkerScheduler(workerId, info.getHost(), info.getPort())).start();
        }
      }
    }
  }

  /** The reader. */
  private static final MyriaConfigurationReader READER = new MyriaConfigurationReader();

  /**
   * The class to launch a new worker during recovery.
   */
  private class NewWorkerScheduler implements Runnable {
    /** the new worker's worker id. */
    private final int workerId;
    /** the new worker's port number id. */
    private final int port;
    /** the new worker's hostname. */
    private final String address;

    /**
     * constructor.
     * 
     * @param workerId worker id.
     * @param address hostname.
     * @param port port number.
     */
    NewWorkerScheduler(final int workerId, final String address, final int port) {
      this.workerId = workerId;
      this.address = address;
      this.port = port;
    }

    @Override
    public void run() {
      try {
        final String temp = Files.createTempDirectory(null).toAbsolutePath().toString();
        Map<String, String> tmpMap = Collections.emptyMap();
        String configFileName = catalog.getConfigurationValue(MyriaSystemConfigKeys.DEPLOYMENT_FILE);
        Map<String, Map<String, String>> config = READER.load(configFileName);
        CatalogMaker.makeOneWorkerCatalog(workerId + "", temp, config, tmpMap, true);

        final String workingDir = config.get("paths").get(workerId + "");
        final String description = catalog.getConfigurationValue(MyriaSystemConfigKeys.DESCRIPTION);
        String remotePath = workingDir;
        if (description != null) {
          remotePath += "/" + description + "-files" + "/" + description;
        }
        DeploymentUtils.mkdir(address, remotePath);
        String localPath = temp + "/" + "worker_" + workerId;
        DeploymentUtils.rsyncFileToRemote(localPath, address, remotePath);
        final String maxHeapSize = config.get("deployment").get("max_heap_size");
        LOGGER.info("starting new worker at {}:{}", address, port);
        boolean debug = config.get("deployment").get("debug_mode").equals("true");
        DeploymentUtils.startWorker(address, workingDir, description, maxHeapSize, workerId + "", port, debug);
      } catch (CatalogException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Master cleanup.
   */
  private void cleanup() {
    LOGGER.info("{} is going to shutdown", MyriaConstants.SYSTEM_NAME);

    if (scheduledWorkers.size() > 0) {
      LOGGER.info("Waiting for scheduled recovery workers, please wait");
      while (scheduledWorkers.size() > 0) {
        try {
          Thread.sleep(MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    for (MasterSubQuery p : executingSubQueries.values()) {
      p.kill();
    }

    if (messageProcessingExecutor != null && !messageProcessingExecutor.isShutdown()) {
      messageProcessingExecutor.shutdownNow();
    }
    if (scheduledTaskExecutor != null && !scheduledTaskExecutor.isShutdown()) {
      scheduledTaskExecutor.shutdownNow();
    }

    /*
     * Close the catalog before shutting down the IPC because there may be Catalog jobs pending that were triggered by
     * IPC events.
     */
    catalog.close();

    while (aliveWorkers.size() > 0) {
      // TODO add process kill
      LOGGER.info("Send shutdown requests to the workers, please wait");
      for (final Integer workerId : aliveWorkers.keySet()) {
        SocketInfo workerAddr = workers.get(workerId);
        LOGGER.info("Shutting down #{} : {}", workerId, workerAddr);
        connectionPool.sendShortMessage(workerId, IPCUtils.CONTROL_SHUTDOWN);
      }

      for (final int workerId : aliveWorkers.keySet()) {
        if (!connectionPool.isRemoteAlive(workerId)) {
          aliveWorkers.remove(workerId);
        }
      }
      if (aliveWorkers.size() > 0) {
        try {
          Thread.sleep(MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    connectionPool.shutdown();
    connectionPool.releaseExternalResources();
    if (ipcPipelineExecutor != null && !ipcPipelineExecutor.isShutdown()) {
      ipcPipelineExecutor.shutdown();
    }
    LOGGER.info("Master connection pool shutdown complete.");

    LOGGER.info("Master finishes cleanup.");
  }

  /**
   * @param mqp the master query
   * @return the query dispatch {@link LocalSubQueryFuture}.
   * @throws DbException if any error occurs.
   */
  private LocalSubQueryFuture dispatchWorkerQueryPlans(final MasterSubQuery mqp) throws DbException {
    // directly set the master part as already received.
    mqp.queryReceivedByWorker(MyriaConstants.MASTER_ID);
    for (final Map.Entry<Integer, SubQueryPlan> e : mqp.getWorkerPlans().entrySet()) {
      final Integer workerID = e.getKey();
      while (!aliveWorkers.containsKey(workerID)) {
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_MS);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
      try {
        connectionPool.sendShortMessage(workerID, IPCUtils.queryMessage(mqp.getSubQueryId(), e.getValue()));
      } catch (final IOException ee) {
        throw new DbException(ee);
      }
    }
    return mqp.getWorkerReceiveFuture();
  }

  /**
   * @return if a query is running.
   * @param queryId queryID.
   */
  public boolean queryCompleted(final long queryId) {
    return !activeQueries.containsKey(queryId);
  }

  /**
   * @return if no query is running.
   */
  public boolean allQueriesCompleted() {
    return activeQueries.isEmpty();
  }

  /**
   * Shutdown the master.
   */
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

    scheduledTaskExecutor.scheduleAtFixedRate(new DebugHelper(), DebugHelper.INTERVAL, DebugHelper.INTERVAL,
        TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(new WorkerLivenessChecker(),
        MyriaConstants.WORKER_LIVENESS_CHECKER_INTERVAL, MyriaConstants.WORKER_LIVENESS_CHECKER_INTERVAL,
        TimeUnit.MILLISECONDS);

    messageProcessingExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master message processor"));
    serverQueryExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master query executor"));

    /**
     * The {@link Executor} who deals with IPC connection setup/cleanup.
     */
    ExecutorService ipcBossExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC boss"));
    /**
     * The {@link Executor} who deals with IPC message delivering and transformation.
     */
    ExecutorService ipcWorkerExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC worker"));

    ipcPipelineExecutor =
        new OrderedMemoryAwareThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1,
            5 * MyriaConstants.MB, 0, MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS, TimeUnit.MILLISECONDS,
            new RenamingThreadFactory("Master Pipeline executor"));

    /**
     * The {@link ChannelFactory} for creating client side connections.
     */
    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime()
            .availableProcessors() * 2 + 1);

    /**
     * The {@link ChannelFactory} for creating server side accepted connections.
     */
    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime()
            .availableProcessors() * 2 + 1);
    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.

    ChannelPipelineFactory serverPipelineFactory =
        new IPCPipelineFactories.MasterServerPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory clientPipelineFactory =
        new IPCPipelineFactories.MasterClientPipelineFactory(connectionPool, getPipelineExecutor());
    ChannelPipelineFactory masterInJVMPipelineFactory =
        new IPCPipelineFactories.MasterInJVMPipelineFactory(connectionPool);

    connectionPool.start(serverChannelFactory, serverPipelineFactory, clientChannelFactory, clientPipelineFactory,
        masterInJVMPipelineFactory, new InJVMLoopbackChannelSink());

    messageProcessingExecutor.submit(new MessageProcessor());
    LOGGER.info("Server started on {}", masterSocketInfo);

    if (getSchema(MyriaConstants.PROFILING_RELATION) == null
        && getDBMS().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
      final Set<Integer> workerIds = workers.keySet();
      importDataset(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA, workerIds);
      importDataset(MyriaConstants.SENT_RELATION, MyriaConstants.SENT_SCHEMA, workerIds);
    }
  }

  /**
   * @return the dbms from {@link #execEnvVars}.
   */
  public String getDBMS() {
    return (String) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM);
  }

  /**
   * @return the input capacity.
   */
  int getInputBufferCapacity() {
    return inputBufferCapacity;
  }

  /**
   * @return the system wide default inuput buffer recover event trigger.
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   */
  int getInputBufferRecoverTrigger() {
    return inputBufferRecoverTrigger;
  }

  /**
   * Kill a query with queryID.
   * 
   * @param queryID the queryID.
   */
  public void killQuery(final long queryID) {
    getQuery(queryID).kill();
  }

  /**
   * Kill a subquery.
   * 
   * @param subQueryId the ID of the subquery to be killed
   */
  protected void killSubQuery(final SubQueryId subQueryId) {
    Preconditions.checkNotNull(subQueryId, "subQueryId");
    MasterSubQuery subQuery = executingSubQueries.get(subQueryId);
    if (subQuery != null) {
      subQuery.kill();
    } else {
      LOGGER.warn("tried to kill subquery {} but it is not executing.", subQueryId);
    }
  }

  /**
   * 
   * Can be only used in test.
   * 
   * @return true if the query plan is accepted and scheduled for execution.
   * @param masterRoot the root operator of the master plan
   * @param workerRoots the roots of the worker part of the plan, {workerID -> RootOperator[]}
   * @throws DbException if any error occurs.
   * @throws CatalogException catalog errors.
   */
  public QueryFuture submitQueryPlan(final RootOperator masterRoot, final Map<Integer, RootOperator[]> workerRoots)
      throws DbException, CatalogException {
    String catalogInfoPlaceHolder = "MasterPlan: " + masterRoot + "; WorkerPlan: " + workerRoots;
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (Entry<Integer, RootOperator[]> entry : workerRoots.entrySet()) {
      workerPlans.put(entry.getKey(), new SubQueryPlan(entry.getValue()));
    }
    return submitQuery(catalogInfoPlaceHolder, catalogInfoPlaceHolder, catalogInfoPlaceHolder, new SubQueryPlan(
        masterRoot), workerPlans, false);
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   * 
   * @param rawQuery the raw user-defined query. E.g., the source Datalog program.
   * @param logicalRa the logical relational algebra of the compiled plan.
   * @param physicalPlan the Myria physical plan for the query.
   * @param workerPlans the physical parallel plan fragments for each worker.
   * @param masterPlan the physical parallel plan fragment for the master.
   * @param profilingMode is the profiling mode of the query on.
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  public QueryFuture submitQuery(final String rawQuery, final String logicalRa, final String physicalPlan,
      final SubQueryPlan masterPlan, final Map<Integer, SubQueryPlan> workerPlans, @Nullable final Boolean profilingMode)
      throws DbException, CatalogException {
    QueryEncoding query = new QueryEncoding();
    query.rawQuery = rawQuery;
    query.logicalRa = rawQuery;
    query.fragments = ImmutableList.of();
    query.profilingMode = Objects.firstNonNull(profilingMode, false);
    return submitQuery(query, new SubQuery(masterPlan, workerPlans));
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   * 
   * @param physicalPlan the Myria physical plan for the query.
   * @param plan the query to be executed
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  public QueryFuture submitQuery(final QueryEncoding query, final QueryPlan plan) throws DbException, CatalogException {
    if (!canSubmitQuery()) {
      throw new DbException("Cannot submit query");
    }
    if (query.profilingMode) {
      if (!(plan instanceof SubQuery || plan instanceof JsonSubQuery)) {
        throw new DbException("Profiling mode is not supported for plans (" + plan.getClass().getSimpleName()
            + ") that may contain multiple subqueries.");
      }
      if (!getDBMS().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        throw new DbException("Profiling mode is only supported when using Postgres as the storage system.");
      }
    }
    if (plan instanceof JsonSubQuery) {
      /* Hack to instantiate a single-fragment query for the visualization. */
      QueryConstruct.instantiate(((JsonSubQuery) plan).getFragments(), new ConstructArgs(this, -1));
    }
    final long queryID = catalog.newQuery(query);
    return submitQuery(queryID, query, plan);
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   * 
   * @param queryId the catalog's assigned ID for this query.
   * @param query contains the query options (profiling, fault tolerance)
   * @param plan the query to be executed
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  private QueryFuture submitQuery(final long queryId, final QueryEncoding query, final QueryPlan plan)
      throws DbException, CatalogException {
    final Query queryState = new Query(queryId, query, plan, this);
    activeQueries.put(queryId, queryState);
    advanceQuery(queryState);
    return queryState.getFuture();
  }

  /**
   * Advance the given query to the next {@link SubQuery}. If there is no next {@link SubQuery}, mark the entire query
   * as having succeeded.
   * 
   * @param queryState the specified query
   * @return the future of the next {@Link SubQuery}, or <code>null</code> if this query has succeeded.
   * @throws DbException if there is an error
   */
  private LocalSubQueryFuture advanceQuery(final Query queryState) throws DbException {
    Verify.verify(queryState.getCurrentSubQuery() == null, "expected queryState current task is null");

    SubQuery task;
    try {
      task = queryState.nextSubQuery();
    } catch (QueryKilledException qke) {
      queryState.markKilled();
      finishQuery(queryState);
      return null;
    } catch (RuntimeException | DbException e) {
      queryState.markFailed(e);
      finishQuery(queryState);
      return null;
    }
    if (task == null) {
      queryState.markSuccess();
      finishQuery(queryState);
      return null;
    }
    return submitSubQuery(queryState);
  }

  /**
   * Finish the specified query by updating its status in the Catalog and then removing it from the active queries.
   * 
   * @param queryState the query to be finished
   * @throws DbException if there is an error updating the Catalog
   */
  private void finishQuery(final Query queryState) throws DbException {
    try {
      catalog.queryFinished(queryState);
    } catch (CatalogException e) {
      throw new DbException("Error finishing query " + queryState.getQueryId(), e);
    } finally {
      activeQueries.remove(queryState.getQueryId());
    }
  }

  /**
   * Submit the next subquery in the query for execution, and return its future.
   * 
   * @param queryState the query containing the subquery to be executed
   * @return the future of the subquery
   * @throws DbException if there is an error submitting the subquery for execution
   */
  private LocalSubQueryFuture submitSubQuery(final Query queryState) throws DbException {
    final SubQuery subQuery =
        Verify.verifyNotNull(queryState.getCurrentSubQuery(), "query state should have a current subquery");
    final SubQueryId subQueryId = subQuery.getSubQueryId();
    try {
      final MasterSubQuery mqp = new MasterSubQuery(subQuery, this);
      executingSubQueries.put(subQueryId, mqp);

      final LocalSubQueryFuture queryExecutionFuture = mqp.getExecutionFuture();

      /* Add the future to update the metadata about created relations, if there are any. */
      queryState.addDatasetMetadataUpdater(catalog, queryExecutionFuture);

      queryExecutionFuture.addListener(new LocalSubQueryFutureListener() {
        @Override
        public void operationComplete(final LocalSubQueryFuture future) throws Exception {

          finishSubQuery(subQueryId);

          final Long elapsedNanos = mqp.getExecutionStatistics().getQueryExecutionElapse();
          if (future.isSuccess()) {
            LOGGER.info("Subquery #{} succeeded. Time elapsed: {}.", subQueryId, DateTimeUtils
                .nanoElapseToHumanReadable(elapsedNanos));
            // TODO success management.
            advanceQuery(queryState);
          } else {
            Throwable cause = future.getCause();
            LOGGER.info("Subquery #{} failed. Time elapsed: {}. Failure cause is {}.", subQueryId, DateTimeUtils
                .nanoElapseToHumanReadable(elapsedNanos), cause);
            if (cause instanceof QueryKilledException) {
              queryState.markKilled();
            } else {
              queryState.markFailed(cause);
            }
            finishQuery(queryState);
          }
        }
      });

      dispatchWorkerQueryPlans(mqp).addListener(new LocalSubQueryFutureListener() {
        @Override
        public void operationComplete(final LocalSubQueryFuture future) throws Exception {
          mqp.init();
          if (subQueryId.getSubqueryId() == 0) {
            getQuery(subQueryId.getQueryId()).markStart();
          }
          mqp.startExecution();
          Server.this.startWorkerQuery(future.getLocalSubQuery().getSubQueryId());
        }
      });

      return mqp.getExecutionFuture();
    } catch (DbException | RuntimeException e) {
      finishSubQuery(subQueryId);
      queryState.markFailed(e);
      finishQuery(queryState);
      throw e;
    }
  }

  /**
   * Finish the subquery by removing it from the data structures.
   * 
   * @param subQueryId the id of the subquery to finish.
   */
  private void finishSubQuery(final SubQueryId subQueryId) {
    long queryId = subQueryId.getQueryId();
    executingSubQueries.remove(subQueryId);
    getQuery(queryId).finishSubQuery();
  }

  /**
   * Tells all the workers to begin executing the specified {@link SubQuery}.
   * 
   * @param subQueryId the id of the subquery to be started.
   */
  private void startWorkerQuery(final SubQueryId subQueryId) {
    final MasterSubQuery mqp = executingSubQueries.get(subQueryId);
    for (final Integer workerID : mqp.getWorkerAssigned()) {
      connectionPool.sendShortMessage(workerID, IPCUtils.startQueryTM(subQueryId));
    }
  }

  /**
   * @return the set of workers that are currently alive.
   */
  public Set<Integer> getAliveWorkers() {
    return ImmutableSet.copyOf(aliveWorkers.keySet());
  }

  /**
   * Return a random subset of workers.
   * 
   * @param number the number of alive workers returned
   * @return a subset of workers that are currently alive.
   */
  public Set<Integer> getRandomWorkers(final int number) {
    Preconditions.checkArgument(number <= getAliveWorkers().size(),
        "The number of workers requested cannot exceed the number of alive workers.");
    if (number == getAliveWorkers().size()) {
      return getAliveWorkers();
    }
    List<Integer> workerList = new ArrayList<>(aliveWorkers.keySet());
    Collections.shuffle(workerList);
    return ImmutableSet.copyOf(workerList.subList(0, number));
  }

  /**
   * @return the set of known workers in this Master.
   */
  public Map<Integer, SocketInfo> getWorkers() {
    return ImmutableMap.copyOf(workers);
  }

  /**
   * Ingest the given dataset.
   * 
   * @param relationKey the name of the dataset.
   * @param workersToIngest restrict the workers to ingest data (null for all)
   * @param indexes the indexes created.
   * @param source the source of tuples to be ingested.
   * @return the status of the ingested dataset.
   * @throws InterruptedException interrupted
   * @throws DbException if there is an error
   */
  public DatasetStatus ingestDataset(final RelationKey relationKey, final Set<Integer> workersToIngest,
      final List<List<IndexRef>> indexes, final Operator source) throws InterruptedException, DbException {
    /* Figure out the workers we will use. If workersToIngest is null, use all active workers. */
    Set<Integer> actualWorkers = workersToIngest;
    if (workersToIngest == null) {
      actualWorkers = getAliveWorkers();
    }
    Preconditions.checkArgument(actualWorkers.size() > 0, "Must use > 0 workers");
    int[] workersArray = MyriaUtils.integerCollectionToIntArray(actualWorkers);

    /* The master plan: send the tuples out. */
    ExchangePairID scatterId = ExchangePairID.newID();
    GenericShuffleProducer scatter =
        new GenericShuffleProducer(source, scatterId, workersArray,
            new RoundRobinPartitionFunction(workersArray.length));

    /* The workers' plan */
    GenericShuffleConsumer gather =
        new GenericShuffleConsumer(source.getSchema(), scatterId, new int[] { MyriaConstants.MASTER_ID });
    DbInsert insert = new DbInsert(gather, relationKey, true, indexes);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    for (Integer workerId : workersArray) {
      workerPlans.put(workerId, new SubQueryPlan(insert));
    }

    ListenableFuture<Query> qf;
    try {
      qf =
          submitQuery("ingest " + relationKey.toString(), "ingest " + relationKey.toString(), "ingest "
              + relationKey.toString(getDBMS()), new SubQueryPlan(scatter), workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException("Error submitting query", e);
    }
    try {
      qf.get();
    } catch (ExecutionException e) {
      throw new DbException("Error executing query", e.getCause());
    }

    return getDatasetStatus(relationKey);
  }

  /**
   * @return whether this master can handle more queries or not.
   */
  public boolean canSubmitQuery() {
    return (activeQueries.size() < MyriaConstants.MAX_ACTIVE_QUERIES);
  }

  /**
   * @param relationKey the relationalKey of the dataset to import
   * @param schema the schema of the dataset to import
   * @param workersToImportFrom the set of workers
   * @throws DbException if there is an error
   * @throws InterruptedException interrupted
   */
  public void importDataset(final RelationKey relationKey, final Schema schema, final Set<Integer> workersToImportFrom)
      throws DbException, InterruptedException {

    /* Figure out the workers we will use. If workersToImportFrom is null, use all active workers. */
    Set<Integer> actualWorkers = workersToImportFrom;
    if (workersToImportFrom == null) {
      actualWorkers = getWorkers().keySet();
    }

    try {
      Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
      for (Integer workerId : actualWorkers) {
        workerPlans.put(workerId, new SubQueryPlan(new SinkRoot(new EOSSource())));
      }
      ListenableFuture<Query> qf =
          submitQuery("import " + relationKey.toString(), "import " + relationKey.toString(), "import "
              + relationKey.toString(getDBMS()), new SubQueryPlan(new SinkRoot(new EOSSource())), workerPlans, false);
      Query queryState;
      try {
        queryState = qf.get();
      } catch (ExecutionException e) {
        throw new DbException("Error executing query", e.getCause());
      }

      /* TODO(dhalperi) -- figure out how to populate the numTuples column. */
      catalog.addRelationMetadata(relationKey, schema, -1, queryState.getQueryId());
      /* Add the round robin-partitioned shard. */
      catalog.addStoredRelation(relationKey, actualWorkers, "RoundRobin");
    } catch (CatalogException e) {
      throw new DbException(e);
    }

  }

  /**
   * @param relationKey the key of the desired relation.
   * @return the schema of the specified relation, or null if not found.
   * @throws CatalogException if there is an error getting the Schema out of the catalog.
   */
  public Schema getSchema(final RelationKey relationKey) throws CatalogException {
    return catalog.getSchema(relationKey);
  }

  /**
   * @param relationKey the key of the desired relation.
   * @param storedRelationId indicates which copy of the desired relation we want to scan.
   * @return the list of workers that store the specified relation.
   * @throws CatalogException if there is an error accessing the catalog.
   */
  public Set<Integer> getWorkersForRelation(final RelationKey relationKey, final Integer storedRelationId)
      throws CatalogException {
    return catalog.getWorkersForRelation(relationKey, storedRelationId);
  }

  /**
   * @param queryId the query that owns the desired temp relation.
   * @param relationKey the key of the desired temp relation.
   * @return the list of workers that store the specified relation.
   */
  public Set<Integer> getWorkersForTempRelation(@Nonnull final Long queryId, @Nonnull final RelationKey relationKey) {
    return getQuery(queryId).getWorkersForTempRelation(relationKey);
  }

  /**
   * @param configKey config key.
   * @return master configuration.
   */
  public String getConfiguration(final String configKey) {
    try {
      return catalog.getConfigurationValue(configKey);
    } catch (CatalogException e) {
      LOGGER.warn("Configuration retrieval error", e);
      return null;
    }
  }

  /**
   * @return the socket info for the master.
   */
  protected SocketInfo getSocketInfo() {
    return masterSocketInfo;
  }

  /**
   * Computes and returns the status of the requested query, or null if the query does not exist.
   * 
   * @param queryId the identifier of the query.
   * @throws CatalogException if there is an error in the catalog.
   * @return the status of this query.
   */
  public QueryStatusEncoding getQueryStatus(final long queryId) throws CatalogException {
    /* Get the stored data for this query, e.g., the submitted program. */
    QueryStatusEncoding queryStatus = catalog.getQuery(queryId);
    if (queryStatus == null) {
      return null;
    }

    Query state = activeQueries.get(queryId);
    if (state == null) {
      /* Not active, so the information from the Catalog is authoritative. */
      return queryStatus;
    }

    /* Currently active, so fill in the latest information about the query. */
    queryStatus.startTime = state.getStartTime();
    queryStatus.finishTime = state.getEndTime();
    queryStatus.elapsedNanos = state.getElapsedTime();
    queryStatus.status = state.getStatus();
    return queryStatus;
  }

  /**
   * Computes and returns the status of queries that have been submitted to Myria.
   * 
   * @param limit the maximum number of results to return. Any value <= 0 is interpreted as all results.
   * @param maxId the largest query ID returned. If null or <= 0, all queries will be returned.
   * @throws CatalogException if there is an error in the catalog.
   * @return a list of the status of every query that has been submitted to Myria.
   */
  public List<QueryStatusEncoding> getQueries(final long limit, final long maxId) throws CatalogException {
    List<QueryStatusEncoding> ret = new LinkedList<>();

    /* Begin by adding the status for all the active queries. */
    TreeSet<Long> activeQueryIds = new TreeSet<>(activeQueries.keySet());
    final Iterator<Long> iter = activeQueryIds.descendingIterator();
    while (iter.hasNext()) {
      long queryId = iter.next();
      final QueryStatusEncoding status = getQueryStatus(queryId);
      if (status == null) {
        LOGGER.warn("Weird: query status for active query {} is null.", queryId);
        continue;
      }
      ret.add(status);
    }

    /* Now add in the status for all the inactive (finished, killed, etc.) queries. */
    for (QueryStatusEncoding q : catalog.getQueries(limit, maxId)) {
      if (!activeQueryIds.contains(q.queryId)) {
        ret.add(q);
      }
    }

    return ret;
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
  public List<DatasetStatus> getDatasetsForProgram(final String userName, final String programName) throws DbException {
    try {
      return catalog.getDatasetsForProgram(userName, programName);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @return number of queries.
   * @throws CatalogException if an error occurs
   */
  public int getNumQueries() throws CatalogException {
    return catalog.getNumQueries();
  }

  /**
   * Start a query that streams tuples from the specified relation to the specified {@link TupleWriter}.
   * 
   * @param relationKey the relation to be downloaded.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   * @return the query future from which the query status can be looked up.
   * @throws DbException if there is an error in the system.
   */
  public ListenableFuture<Query> startDataStream(final RelationKey relationKey, final TupleWriter writer)
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
      scanWorkers = getWorkersForRelation(relationKey, null);
    } catch (CatalogException e) {
      throw new DbException(e);
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
    final CollectConsumer consumer = new CollectConsumer(schema, operatorId, ImmutableSet.copyOf(scanWorkers));
    DataOutput output = new DataOutput(consumer, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString = "download " + relationKey.toString();
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param writer writer to get data.
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public ListenableFuture<Query> startSentLogDataStream(final long queryId, final long fragmentId,
      final TupleWriter writer) throws DbException {
    final QueryStatusEncoding queryStatus = checkAndReturnQueryStatus(queryId);

    Set<Integer> actualWorkers = ((QueryEncoding) queryStatus.plan).getWorkers();

    String fragmentWhere = "";
    if (fragmentId >= 0) {
      fragmentWhere = "AND fragmentid = " + fragmentId;
    }

    final Schema schema =
        Schema.ofFields("fragmentid", Type.INT_TYPE, "destworker", Type.INT_TYPE, "numTuples", Type.LONG_TYPE);

    String sentQueryString =
        Joiner.on(' ').join("SELECT fragmentid, destworkerid, sum(numtuples) as numtuples FROM",
            MyriaConstants.SENT_RELATION.toString(getDBMS()) + "WHERE queryid =", queryId, fragmentWhere,
            "GROUP BY queryid, fragmentid, destworkerid");

    DbQueryScan scan = new DbQueryScan(sentQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    ImmutableList.Builder<Expression> emitExpressions = ImmutableList.builder();

    emitExpressions.add(new Expression("workerId", new WorkerIdExpression()));

    for (int column = 0; column < schema.numColumns(); column++) {
      VariableExpression copy = new VariableExpression(column);
      emitExpressions.add(new Expression(schema.getColumnName(column), copy));
    }

    Apply addWorkerId = new Apply(scan, emitExpressions.build());

    CollectProducer producer = new CollectProducer(addWorkerId, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    final CollectConsumer consumer =
        new CollectConsumer(addWorkerId.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    final MultiGroupByAggregate aggregate =
        new MultiGroupByAggregate(consumer, new int[] { 0, 1, 2 }, new int[] { 3 }, new int[] { Aggregator.AGG_OP_SUM });

    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("src", new VariableExpression(0)));
    renameExpressions.add(new Expression("fragmentId", new VariableExpression(1)));
    renameExpressions.add(new Expression("dest", new VariableExpression(2)));
    renameExpressions.add(new Expression("numTuples", new VariableExpression(3)));
    final Apply rename = new Apply(aggregate, renameExpressions.build());

    DataOutput output = new DataOutput(rename, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on("").join("download profiling log data for (query=", queryId, ", fragment=", fragmentId, ")");
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param start the earliest time where we need data
   * @param end the latest time
   * @param minSpanLength minimum length of a span to be returned
   * @param onlyRootOperator only return data for root operator
   * @param writer writer to get data.
   * @return profiling logs for the query.
   * 
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startLogDataStream(final long queryId, final long fragmentId, final long start, final long end,
      final long minSpanLength, final boolean onlyRootOperator, final TupleWriter writer) throws DbException {
    final QueryStatusEncoding queryStatus = checkAndReturnQueryStatus(queryId);

    Preconditions.checkArgument(start < end, "range cannot be negative");

    final Schema schema =
        Schema.ofFields("opId", Type.INT_TYPE, "startTime", Type.LONG_TYPE, "endTime", Type.LONG_TYPE, "numTuples",
            Type.LONG_TYPE);

    Set<Integer> actualWorkers = ((QueryEncoding) queryStatus.plan).getWorkers();

    String opCondition = "";
    if (onlyRootOperator) {
      opCondition =
          Joiner.on(' ').join("AND opid = (SELECT opid FROM", MyriaConstants.PROFILING_RELATION.toString(getDBMS()),
              "WHERE", fragmentId, "=fragmentId AND", queryId, "=queryId ORDER BY starttime ASC limit 1)");;
    }

    String spanCondition = "";
    if (minSpanLength > 0) {
      spanCondition = Joiner.on(' ').join("AND endtime - starttime >", minSpanLength);
    }

    String queryString =
        Joiner.on(' ').join("SELECT opid, starttime, endtime, numtuples FROM",
            MyriaConstants.PROFILING_RELATION.toString(getDBMS()), "WHERE fragmentId =", fragmentId, "AND queryid =",
            queryId, "AND endtime >", start, "AND starttime <", end, opCondition, spanCondition,
            "ORDER BY starttime ASC");

    DbQueryScan scan = new DbQueryScan(queryString, schema);

    ImmutableList.Builder<Expression> emitExpressions = ImmutableList.builder();

    emitExpressions.add(new Expression("workerId", new WorkerIdExpression()));

    for (int column = 0; column < schema.numColumns(); column++) {
      VariableExpression copy = new VariableExpression(column);
      emitExpressions.add(new Expression(schema.getColumnName(column), copy));
    }

    Apply addWorkerId = new Apply(scan, emitExpressions.build());

    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(addWorkerId, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    final CollectConsumer consumer =
        new CollectConsumer(addWorkerId.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    DataOutput output = new DataOutput(consumer, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on('\0').join("download profiling data (query=", queryId, ", fragment=", fragmentId, ", range=[",
            Joiner.on(", ").join(start, end), "]", ")");
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param start start of the histogram
   * @param end the end of the histogram
   * @param step the step size between min and max
   * @param onlyRootOp return histogram only for root operator
   * @param writer writer to get data.
   * @return profiling logs for the query.
   * 
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startHistogramDataStream(final long queryId, final long fragmentId, final long start,
      final long end, final long step, final boolean onlyRootOp, final TupleWriter writer) throws DbException {
    final QueryStatusEncoding queryStatus = checkAndReturnQueryStatus(queryId);

    Preconditions.checkArgument(start < end, "range cannot be negative");
    Preconditions.checkArgument(step > 0, "step has to be greater than 0");

    final Schema schema = Schema.ofFields("opId", Type.INT_TYPE, "nanoTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.PROFILING_RELATION;

    Set<Integer> actualWorkers = ((QueryEncoding) queryStatus.plan).getWorkers();

    String filterOpnameQueryString = "";
    if (onlyRootOp) {
      filterOpnameQueryString =
          Joiner.on(' ').join("AND p.opid=(SELECT opid FROM", relationKey.toString(getDBMS()), "WHERE fragmentid=",
              fragmentId, " AND queryid=", queryId, "ORDER BY starttime ASC limit 1)");
    }

    String histogramWorkerQueryString =
        Joiner.on(' ').join("SELECT p.opid, s.t AS nanotime FROM generate_series(", start, ", ", end, ", ", step,
            ") AS s(t), ", relationKey.toString(getDBMS()), " AS p WHERE p.queryid=", queryId, "AND p.fragmentid=",
            fragmentId, filterOpnameQueryString, "AND s.t BETWEEN p.starttime AND p.endtime");

    DbQueryScan scan = new DbQueryScan(histogramWorkerQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Aggregate histogram on master */
    final CollectConsumer consumer =
        new CollectConsumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // sum up the number of workers working
    final MultiGroupByAggregate sumAggregate =
        new MultiGroupByAggregate(consumer, new int[] { 0, 1 }, new int[] { 1 }, new int[] { Aggregator.AGG_OP_COUNT });
    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("opId", new VariableExpression(0)));
    renameExpressions.add(new Expression("nanoTime", new VariableExpression(1)));
    renameExpressions.add(new Expression("numWorkers", new VariableExpression(2)));
    final Apply rename = new Apply(sumAggregate, renameExpressions.build());

    DataOutput output = new DataOutput(rename, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on('\0').join("download profiling histogram (query=", queryId, ", fragment=", fragmentId, ", range=[",
            Joiner.on(", ").join(start, end, step), "]", ")");
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId the query id
   * @param fragmentId the fragment id
   * @param writer writer to get data
   * @return profiling logs for the query.
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startRangeDataStream(final Long queryId, final Long fragmentId, final TupleWriter writer)
      throws DbException {
    final QueryStatusEncoding queryStatus = checkAndReturnQueryStatus(queryId);

    final Schema schema = Schema.ofFields("startTime", Type.LONG_TYPE, "endTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.PROFILING_RELATION;

    Set<Integer> actualWorkers = ((QueryEncoding) queryStatus.plan).getWorkers();

    String opnameQueryString =
        Joiner.on(' ').join("SELECT min(starttime), max(endtime) FROM", relationKey.toString(getDBMS()),
            "WHERE queryid=", queryId, "AND fragmentid=", fragmentId);

    DbQueryScan scan = new DbQueryScan(opnameQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Construct the master plan. */
    final CollectConsumer consumer =
        new CollectConsumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // Aggregate range on master
    final Aggregate sumAggregate =
        new Aggregate(consumer, new int[] { 0, 1 }, new int[] { Aggregator.AGG_OP_MIN, Aggregator.AGG_OP_MAX });

    DataOutput output = new DataOutput(sumAggregate, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString = Joiner.on('\0').join("download time range (query=", queryId, ", fragment=", fragmentId, ")");
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * @param queryId query id.
   * @param fragmentId the fragment id to return data for. All fragments, if < 0.
   * @param writer writer to get data.
   * @return contributions for operator.
   * 
   * @throws DbException if there is an error when accessing profiling logs.
   */
  public QueryFuture startContributionsStream(final long queryId, final long fragmentId, final TupleWriter writer)
      throws DbException {
    final QueryStatusEncoding queryStatus = checkAndReturnQueryStatus(queryId);

    final Schema schema = Schema.ofFields("opId", Type.INT_TYPE, "nanoTime", Type.LONG_TYPE);
    final RelationKey relationKey = MyriaConstants.PROFILING_RELATION;

    Set<Integer> actualWorkers = ((QueryEncoding) queryStatus.plan).getWorkers();

    String fragIdCondition = "";
    if (fragmentId >= 0) {
      fragIdCondition = "AND fragmentid=" + fragmentId;
    }

    String opContributionsQueryString =
        Joiner.on(' ').join("SELECT opid, sum(endtime - starttime) FROM ", relationKey.toString(getDBMS()),
            "WHERE queryid=", queryId, fragIdCondition, "GROUP BY opid");

    DbQueryScan scan = new DbQueryScan(opContributionsQueryString, schema);
    final ExchangePairID operatorId = ExchangePairID.newID();

    CollectProducer producer = new CollectProducer(scan, operatorId, MyriaConstants.MASTER_ID);

    SubQueryPlan workerPlan = new SubQueryPlan(producer);
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>(actualWorkers.size());
    for (Integer worker : actualWorkers) {
      workerPlans.put(worker, workerPlan);
    }

    /* Aggregate on master */
    final CollectConsumer consumer =
        new CollectConsumer(scan.getSchema(), operatorId, ImmutableSet.copyOf(actualWorkers));

    // sum up contributions
    final SingleGroupByAggregate sumAggregate =
        new SingleGroupByAggregate(consumer, new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_SUM });

    // rename columns
    ImmutableList.Builder<Expression> renameExpressions = ImmutableList.builder();
    renameExpressions.add(new Expression("opId", new VariableExpression(0)));
    renameExpressions.add(new Expression("nanoTime", new VariableExpression(1)));
    final Apply rename = new Apply(sumAggregate, renameExpressions.build());

    DataOutput output = new DataOutput(rename, writer);
    final SubQueryPlan masterPlan = new SubQueryPlan(output);

    /* Submit the plan for the download. */
    String planString =
        Joiner.on('\0').join("download operator contributions (query=", queryId, ", fragment=", fragmentId, ")");
    try {
      return submitQuery(planString, planString, planString, masterPlan, workerPlans, false);
    } catch (CatalogException e) {
      throw new DbException(e);
    }
  }

  /**
   * Get the query status and check whether the query ran successfully with profiling enabled.
   * 
   * @param queryId the query id
   * @return the query status
   * @throws DbException if the query cannot be retrieved
   */
  private QueryStatusEncoding checkAndReturnQueryStatus(final long queryId) throws DbException {
    /* Get the relation's schema, to make sure it exists. */
    final QueryStatusEncoding queryStatus;
    try {
      queryStatus = catalog.getQuery(queryId);
    } catch (CatalogException e) {
      throw new DbException(e);
    }

    Preconditions.checkArgument(queryStatus != null, "query %s not found", queryId);
    Preconditions.checkArgument(queryStatus.status == QueryStatusEncoding.Status.SUCCESS,
        "query %s did not succeed (%s)", queryId, queryStatus.status);
    Preconditions.checkArgument(queryStatus.profilingMode, "query %s was not run with profiling enabled", queryId);
    return queryStatus;
  }

  /**
   * Update the {@link MasterCatalog} so that the specified relation has the specified tuple count.
   * 
   * @param relation the relation to update
   * @param count the number of tuples in that relation
   * @throws DbException if there is an error in the catalog
   */
  public void updateRelationTupleCount(final RelationKey relation, final long count) throws DbException {
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
  public void setQueryGlobal(final long queryId, @Nonnull final String key, @Nonnull final Object value) {
    Preconditions.checkNotNull(key, "key");
    Preconditions.checkNotNull(value, "value");
    getQuery(queryId).setGlobal(key, value);
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
    return getQuery(queryId).getGlobal(key);
  }

  /**
   * Return the schema of the specified temp relation in the specified query.
   * 
   * @param queryId the query that owns the temp relation
   * @param name the name of the temporary relation
   * @return the schema of the specified temp relation in the specified query
   */
  public Schema getTempSchema(@Nonnull final Long queryId, @Nonnull final String name) {
    return getQuery(queryId).getTempSchema(RelationKey.ofTemp(queryId, name));
  }

  /**
   * Get the query with the specified ID, ensuring that it is active.
   * 
   * @param queryId the id of the query to return.
   * @return the query with the specified ID, ensuring that it is active
   * @throws IllegalArgumentException if there is no active query with the given ID
   */
  @Nonnull
  private Query getQuery(@Nonnull final Long queryId) {
    Query query = activeQueries.get(Preconditions.checkNotNull(queryId, "queryId"));
    Preconditions.checkArgument(query != null, "Query #%s is not active", queryId);
    return query;
  }
}
