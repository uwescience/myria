package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.coordinator.catalog.Catalog;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.parallel.ExchangeData.MetaMessage;
import edu.washington.escience.myriad.parallel.MasterDataHandler.MessageWrapper;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.InJVMLoopbackChannelSink;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.QueryProto.QueryMessage;
import edu.washington.escience.myriad.proto.QueryProto.QueryReport;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.DateTimeUtils;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.MyriaUtils;

/**
 * The master entrance.
 * */
public final class Server {

  /**
   * Master message processor.
   * */
  private final class MessageProcessor implements Runnable {

    /** Constructor, set the thread name. */
    public MessageProcessor() {
      super();
    }

    @Override
    public void run() {
      TERMINATE_MESSAGE_PROCESSING : while (true) {
        MessageWrapper mw = null;
        try {
          mw = messageQueue.take();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break TERMINATE_MESSAGE_PROCESSING;
        }

        final TransportMessage m = mw.getMessage();
        final int senderID = mw.getSenderID();

        switch (m.getType()) {
          case DATA:
            final DataMessage data = m.getDataMessage();
            final long exchangePairIDLong = data.getOperatorID();
            final ExchangePairID exchangePairID = ExchangePairID.fromExisting(exchangePairIDLong);
            ConsumerChannel cc = consumerChannelMap.get(new ExchangeChannelID(exchangePairIDLong, senderID));
            final Schema operatorSchema = cc.getOwnerConsumer().getSchema();
            switch (data.getType()) {
              case EOS:
                LOGGER.debug("EOS from: " + senderID + "," + workers.get(senderID));
                receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, MetaMessage.EOS));
                cc.getOwnerTask().notifyNewInput();
                break;
              case EOI:
                receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, MetaMessage.EOI));
                cc.getOwnerTask().notifyNewInput();

                break;
              case NORMAL:
                final List<ColumnMessage> columnMessages = data.getColumnsList();
                final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
                int idx = 0;
                for (final ColumnMessage cm : columnMessages) {
                  columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, data.getNumTuples());
                }
                final List<Column<?>> columns = Arrays.asList(columnArray);
                receiveData((new ExchangeData(exchangePairID, senderID, columns, operatorSchema, data.getNumTuples(),
                    data.getSeq())));
                cc.getOwnerTask().notifyNewInput();
                break;
              case BOS:
                /* ignored */
                break;
            }
            break;
          case CONTROL:
            final ControlMessage controlM = m.getControlMessage();
            switch (controlM.getType()) {
              case WORKER_ALIVE:
                aliveWorkers.add(senderID);
                break;
              case DISCONNECT:
                /* TODO */
                break;
              default:
                if (LOGGER.isErrorEnabled()) {
                  LOGGER.error("Unexpected control message received at master: " + controlM);
                }
            }
            break;
          case QUERY:
            final QueryMessage qm = m.getQueryMessage();
            final long queryId = qm.getQueryId();
            switch (qm.getType()) {
              case QUERY_READY_TO_EXECUTE:
                MasterQueryPartition mqp = activeQueries.get(queryId);
                mqp.queryReceivedByWorker(senderID);
                break;
              case QUERY_COMPLETE:
                mqp = activeQueries.get(queryId);
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
                    if (LOGGER.isErrorEnabled()) {
                      LOGGER.error("Error decoding failure cause", e);
                    }
                  }
                  mqp.workerFail(senderID, cause);

                  if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Worker #{} failed in executing query #{}.", senderID, queryId, cause);
                  }

                  if (!(cause instanceof QueryKilledException)) {
                    // if any worker fails because of some exception, kill the query.
                    mqp.kill();
                  }

                }
                break;
              default:
                if (LOGGER.isErrorEnabled()) {
                  LOGGER.error("Unexpected query message received at master: " + qm);
                }
                break;
            }

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
   * */
  private final ConcurrentHashMap<Integer, SocketInfo> workers;

  /**
   * Queries currently in execution.
   * */
  private final ConcurrentHashMap<Long, MasterQueryPartition> activeQueries;

  /**
   * Current alive worker set.
   * */
  private final Set<Integer> aliveWorkers;

  /**
   * Execution environment variables for operators.
   * */
  private final ConcurrentHashMap<String, Object> execEnvVars;

  /**
   * The I/O buffer, all the ExchangeMessages sent to the server are buffered here.
   */
  private final ConcurrentHashMap<ExchangePairID, InputBuffer<TupleBatch, ExchangeData>> dataBuffer;

  /**
   * @return all input buffers currently in use.
   * */
  ConcurrentHashMap<ExchangePairID, InputBuffer<TupleBatch, ExchangeData>> getDataBuffer() {
    return dataBuffer;
  }

  /**
   * All message queue.
   * 
   * @TODO remove this queue as in {@link Worker}s.
   * */
  private final LinkedBlockingQueue<MasterDataHandler.MessageWrapper> messageQueue;

  /**
   * The IPC Connection Pool.
   * */
  private final IPCConnectionPool connectionPool;

  /**
   * {@link ExecutorService} for message processing.
   * */
  private volatile ExecutorService messageProcessingExecutor;

  /** The Catalog stores the metadata about the Myria instance. */
  private final Catalog catalog;

  /**
   * IPC flow controller.
   * */
  private final FlowControlHandler flowController;

  /**
   * IPC message handler.
   * */
  private final MasterDataHandler masterDataHandler;

  /**
   * Default input buffer capacity for {@link Consumer} input buffers.
   * */
  private final int inputBufferCapacity;

  /**
   * The {@link OrderedMemoryAwareThreadPoolExecutor} who gets messages from {@link workerExecutor} and further process
   * them using application specific message handlers, e.g. {@link MasterDataHandler}.
   * */
  private volatile OrderedMemoryAwareThreadPoolExecutor ipcPipelineExecutor;

  /**
   * The {@link ExecutorService} who executes the master-side query partitions.
   * */
  private volatile ExecutorService serverQueryExecutor;

  /**
   * @return the query executor used in this worker.
   * */
  ExecutorService getQueryExecutor() {
    return serverQueryExecutor;
  }

  /**
   * Producer channel mapping of current active queries.
   * */
  private final ConcurrentHashMap<ExchangeChannelID, ProducerChannel> producerChannelMap;

  /**
   * Consumer channel mapping of current active queries.
   * */
  private final ConcurrentHashMap<ExchangeChannelID, ConsumerChannel> consumerChannelMap;

  /**
   * max number of seconds for elegant cleanup.
   * */
  public static final int NUM_SECONDS_FOR_ELEGANT_CLEANUP = 10;

  /**
   * Entry point for the Master.
   * 
   * @param args the command line arguments.
   * @throws IOException if there's any error in reading catalog file.
   * */
  public static void main(final String[] args) throws IOException {
    try {

      Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
      Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

      if (args.length < 1) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error(USAGE);
        }
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
              if (LOGGER.isInfoEnabled()) {
                LOGGER
                    .info("Wait for " + Server.NUM_SECONDS_FOR_ELEGANT_CLEANUP + " seconds for graceful cleaning-up.");
              }
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
                if (LOGGER.isInfoEnabled()) {
                  LOGGER.info("Graceful cleaning-up timeout. Going to shutdown abruptly.");
                }
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
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Unknown error occurs at Master. Quit directly.", e);
      }
    }
  }

  /**
   * @return my flow controller.
   * */
  FlowControlHandler getFlowControlHandler() {
    return flowController;
  }

  /**
   * @return my message processor.
   * */
  MasterDataHandler getDataHandler() {
    return masterDataHandler;
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
    return ipcPipelineExecutor;
  }

  /** The socket info for the master. */
  private final SocketInfo masterSocketInfo;

  /**
   * @return my execution environment variables for init of operators.
   * */
  ConcurrentHashMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * Construct a server object, with configuration stored in the specified catalog file.
   * 
   * @param catalogFileName the name of the file containing the catalog.
   * @throws FileNotFoundException the specified file not found.
   * @throws CatalogException if there is an error reading from the Catalog.
   */
  public Server(final String catalogFileName) throws FileNotFoundException, CatalogException {
    catalog = Catalog.open(catalogFileName);

    /* Get the master socket info */
    List<SocketInfo> masters = catalog.getMasters();
    if (masters.size() != 1) {
      throw new RuntimeException("Unexpected number of masters: expected 1, got " + masters.size());
    }
    masterSocketInfo = masters.get(MyriaConstants.MASTER_ID);

    workers = new ConcurrentHashMap<Integer, SocketInfo>(catalog.getWorkers());
    final ImmutableMap<String, String> allConfigurations = catalog.getAllConfigurations();

    inputBufferCapacity =
        Integer.valueOf(catalog.getConfigurationValue(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY));
    execEnvVars = new ConcurrentHashMap<String, Object>();
    for (Entry<String, String> cE : allConfigurations.entrySet()) {
      execEnvVars.put(cE.getKey(), cE.getValue());
    }

    aliveWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
    activeQueries = new ConcurrentHashMap<Long, MasterQueryPartition>();

    dataBuffer = new ConcurrentHashMap<ExchangePairID, InputBuffer<TupleBatch, ExchangeData>>();
    messageQueue = new LinkedBlockingQueue<MessageWrapper>();

    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>(workers);
    computingUnits.put(MyriaConstants.MASTER_ID, masterSocketInfo);

    masterDataHandler = new MasterDataHandler(messageQueue);
    producerChannelMap = new ConcurrentHashMap<ExchangeChannelID, ProducerChannel>();
    consumerChannelMap = new ConcurrentHashMap<ExchangeChannelID, ConsumerChannel>();
    flowController = new FlowControlHandler(consumerChannelMap, producerChannelMap);

    connectionPool =
        new IPCConnectionPool(MyriaConstants.MASTER_ID, computingUnits, IPCConfigurations
            .createMasterIPCServerBootstrap(this), IPCConfigurations.createMasterIPCClientBootstrap(this));

    execEnvVars.put("ipcConnectionPool", connectionPool);

    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(new RenamingThreadFactory("Master global timer"));

  }

  /**
   * timer task executor.
   * */
  private final ScheduledExecutorService scheduledTaskExecutor;

  /**
   * This class presents only for the purpose of debugging. No other usage.
   * */
  public class DebugHelper extends TimerTask {

    /**
     * Interval of execution.
     * */
    public static final int INTERVAL = MyriaConstants.WAITING_INTERVAL_1_SECOND_IN_MS;
    /**
     * No use. only for debugging.
     * */
    private int i;

    /**
     * for removing the damn check style warning.
     * 
     * @return meaningless integer.
     * */
    public final int getI() {
      return i;
    }

    @Override
    public final synchronized void run() {
      if (System.currentTimeMillis() > 0) {
        i++;
      }
    }
  }

  /**
   * Master cleanup.
   * */
  private void cleanup() {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(MyriaConstants.SYSTEM_NAME + " is going to shutdown");
    }
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Send shutdown requests to the workers, please wait");
    }
    messageProcessingExecutor.shutdownNow();
    scheduledTaskExecutor.shutdownNow();
    if (!connectionPool.isShutdown()) {
      for (final Integer workerId : aliveWorkers) {
        SocketInfo workerAddr = workers.get(workerId);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Shutting down #{} : {}", workerId, workerAddr);
        }

        ChannelFuture cf = null;
        try {
          cf = connectionPool.sendShortMessage(workerId, IPCUtils.CONTROL_SHUTDOWN);
        } catch (IllegalStateException e) {
          // connection pool is already shutdown.
          cf = null;
        }
        if (cf == null) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Fail to connect the worker{ id:{},address:{} }. Continue cleaning", workerId, workerAddr);
          }
        } else {
          try {
            cf.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Done for worker #{}", workerId);
          }
        }
      }
      try {
        connectionPool.shutdown().await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Connection pool cleanup interrupted");
        }
      }
      connectionPool.releaseExternalResources();
      if (ipcPipelineExecutor != null && !ipcPipelineExecutor.isShutdown()) {
        ipcPipelineExecutor.shutdown();
      }

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Master connection pool shutdown complete.");
      }
    }
    catalog.close();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Master finishes cleanup.");
    }
  }

  /**
   * @param mqp the master query
   * @return the query dispatch {@link QueryFuture}.
   * @throws DbException if any error occurs.
   * */
  private QueryFuture dispatchWorkerQueryPlans(final MasterQueryPartition mqp) throws DbException {
    // directly set the master part as already received.
    mqp.queryReceivedByWorker(MyriaConstants.MASTER_ID);
    for (final Map.Entry<Integer, RootOperator[]> e : mqp.getWorkerPlans().entrySet()) {
      final Integer workerID = e.getKey();
      while (!aliveWorkers.contains(workerID)) {
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_MS);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
      try {
        connectionPool.sendShortMessage(workerID, IPCUtils.queryMessage(mqp.getQueryID(), e.getValue()));
      } catch (final IOException ee) {
        throw new DbException(ee);
      }
    }
    return mqp.getWorkerReceiveFuture();
  }

  /**
   * Callback method when a data item is received.
   * 
   * @param data the data that was received.
   */
  private void receiveData(final ExchangeData data) {
    InputBuffer<TupleBatch, ExchangeData> q = null;
    q = dataBuffer.get(data.getOperatorID());
    if (q != null) {
      q.offer(data);
    } else {
      LOGGER.warn("weird: got ExchangeData (" + data + ") on null q");
    }
  }

  /**
   * @return if a query is running.
   * @param queryId queryID.
   * */
  public boolean queryCompleted(final long queryId) {
    return !activeQueries.containsKey(queryId);
  }

  /**
   * Shutdown the master.
   * */
  public void shutdown() {
    cleanup();
  }

  /**
   * Start all the threads that do work for the server.
   * 
   * @throws Exception if any error occurs.
   */
  public void start() throws Exception {
    LOGGER.info("Server starting on {}", masterSocketInfo.toString());

    scheduledTaskExecutor.scheduleAtFixedRate(new DebugHelper(), DebugHelper.INTERVAL, DebugHelper.INTERVAL,
        TimeUnit.MILLISECONDS);

    messageProcessingExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master message processor"));
    serverQueryExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master query executor"));

    /**
     * The {@link Executor} who deals with IPC connection setup/cleanup.
     * */
    ExecutorService ipcBossExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC boss"));
    /**
     * The {@link Executor} who deals with IPC message delivering and transformation.
     * */
    ExecutorService ipcWorkerExecutor = Executors.newCachedThreadPool(new RenamingThreadFactory("Master IPC worker"));

    ipcPipelineExecutor =
        new OrderedMemoryAwareThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1, 0, 0,
            MyriaConstants.THREAD_POOL_KEEP_ALIVE_TIME_IN_MS, TimeUnit.MILLISECONDS, new RenamingThreadFactory(
                "Master Pipeline executor"));

    /**
     * The {@link ChannelFactory} for creating client side connections.
     * */
    ChannelFactory clientChannelFactory =
        new NioClientSocketChannelFactory(ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime()
            .availableProcessors() * 2 + 1);

    /**
     * The {@link ChannelFactory} for creating server side accepted connections.
     * */
    ChannelFactory serverChannelFactory =
        new NioServerSocketChannelFactory(ipcBossExecutor, ipcWorkerExecutor, Runtime.getRuntime()
            .availableProcessors() * 2 + 1);
    // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.

    ChannelPipelineFactory serverPipelineFactory = new IPCPipelineFactories.MasterServerPipelineFactory(this);
    ChannelPipelineFactory clientPipelineFactory = new IPCPipelineFactories.MasterClientPipelineFactory(this);
    ChannelPipelineFactory masterInJVMPipelineFactory = new IPCPipelineFactories.MasterInJVMPipelineFactory(this);

    connectionPool.start(serverChannelFactory, serverPipelineFactory, clientChannelFactory, clientPipelineFactory,
        masterInJVMPipelineFactory, new InJVMLoopbackChannelSink());

    messageProcessingExecutor.submit(new MessageProcessor());
    LOGGER.info("Server started on {}", masterSocketInfo.toString());
  }

  /**
   * @return the input capacity.
   * */
  int getInputBufferCapacity() {
    return inputBufferCapacity;
  }

  /**
   * Pause a query with queryID.
   * 
   * @param queryID the queryID.
   * @return the future instance of the pause action.
   * */
  public QueryFuture pauseQuery(final long queryID) {
    return activeQueries.get(queryID).pause();
  }

  /**
   * Pause a query with queryID.
   * 
   * @param queryID the queryID.
   * */
  public void killQuery(final long queryID) {
    activeQueries.get(queryID).kill();
  }

  /**
   * Pause a query with queryID.
   * 
   * @param queryID the queryID.
   * @return the future instance of the resume action.
   * */
  public QueryFuture resumeQuery(final long queryID) {
    return activeQueries.get(queryID).resume();
  }

  /**
   * 
   * @return true if the query plan is accepted and scheduled for execution.
   * @param masterPlan the master part of the plan
   * @param workerPlans the worker part of the plan, {workerID -> RootOperator[]}
   * @throws DbException if any error occurs.
   * @throws CatalogException catalog errors.
   * */
  public QueryFuture submitQueryPlan(final RootOperator masterPlan, final Map<Integer, RootOperator[]> workerPlans)
      throws DbException, CatalogException {
    String catalogInfoPlaceHolder = "MasterPlan: " + masterPlan + "; WorkerPlan: " + workerPlans;
    return submitQuery(catalogInfoPlaceHolder, catalogInfoPlaceHolder, masterPlan, workerPlans);
  }

  /**
   * Submit a query for execution. The plans may be removed in the future if the query compiler and schedulers are ready
   * such that the plan can be generated from either the rawQuery or the logicalRa.
   * 
   * @param rawQuery the raw user-defined query. E.g., the source Datalog program.
   * @param logicalRa the logical relational algebra of the compiled plan.
   * @param plans the physical parallel plan fragments for each worker and the master.
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   * */
  public QueryFuture submitQuery(final String rawQuery, final String logicalRa, final Map<Integer, RootOperator[]> plans)
      throws DbException, CatalogException {
    RootOperator masterPlan = plans.get(MyriaConstants.MASTER_ID)[0];
    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>(plans);
    workerPlans.remove(MyriaConstants.MASTER_ID);
    return this.submitQuery(rawQuery, logicalRa, masterPlan, workerPlans);
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready.
   * 
   * @param rawQuery the raw user-defined query. E.g., the source Datalog program.
   * @param logicalRa the logical relational algebra of the compiled plan.
   * @param workerPlans the physical parallel plan fragments for each worker.
   * @param masterPlan the physical parallel plan fragment for the master.
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   * */
  public QueryFuture submitQuery(final String rawQuery, final String logicalRa, final RootOperator masterPlan,
      final Map<Integer, RootOperator[]> workerPlans) throws DbException, CatalogException {
    workerPlans.remove(MyriaConstants.MASTER_ID);
    final long queryID = catalog.newQuery(rawQuery, logicalRa);
    final MasterQueryPartition mqp = new MasterQueryPartition(masterPlan, workerPlans, queryID, this);
    consumerChannelMap.putAll(mqp.getConsumerChannelMapping());
    producerChannelMap.putAll(mqp.getProducerChannelMapping());

    activeQueries.put(queryID, mqp);

    mqp.getQueryExecutionFuture().addListener(new QueryFutureListener() {
      @Override
      public void operationComplete(final QueryFuture future) throws Exception {

        activeQueries.remove(mqp.getQueryID());
        for (ExchangeChannelID consumerChannelID : mqp.getConsumerChannelMapping().keySet()) {
          consumerChannelMap.remove(consumerChannelID);
        }

        for (ExchangeChannelID producerChannelID : mqp.getProducerChannelMapping().keySet()) {
          producerChannelMap.remove(producerChannelID);
        }

        if (future.isSuccess()) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("The query #{} succeeds. Time elapse: {}.", queryID, DateTimeUtils
                .nanoElapseToHumanReadable(mqp.getExecutionStatistics().getQueryExecutionElapse()));
          }
          // TODO success management.
        } else {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("The query #{} failes. Time elapse: {}. Failure cause is {}.", queryID, DateTimeUtils
                .nanoElapseToHumanReadable(mqp.getExecutionStatistics().getQueryExecutionElapse()), future.getCause());
          }
          // TODO failure management.
        }
      }
    });

    dispatchWorkerQueryPlans(mqp).addListener(new QueryFutureListener() {
      @Override
      public void operationComplete(final QueryFuture future) throws Exception {
        mqp.init();
        mqp.startNonBlockingExecution();
        Server.this.startWorkerQuery(future.getQuery().getQueryID());
      }
    });

    return mqp.getQueryExecutionFuture();
  }

  /**
   * Tells all the workers to start the given query.
   * 
   * @param queryId the id of the query to be started.
   */
  private void startWorkerQuery(final long queryId) {
    final MasterQueryPartition mqp = activeQueries.get(queryId);
    for (final Integer workerID : mqp.getWorkerAssigned()) {
      connectionPool.sendShortMessage(workerID, IPCUtils.startQueryTM(queryId));
    }
  }

  /**
   * @return the set of workers that are currently alive.
   */
  public Set<Integer> getAliveWorkers() {
    return ImmutableSet.copyOf(aliveWorkers);
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
   * @param source the source of tuples to be ingested.
   * @throws InterruptedException interrupted
   * @throws DbException if there is an error
   */
  public void ingestDataset(final RelationKey relationKey, final Set<Integer> workersToIngest, final Operator source)
      throws InterruptedException, DbException {
    /* Figure out the workers we will use. If workersToIngest is null, use all active workers. */
    Set<Integer> actualWorkers = workersToIngest;
    if (workersToIngest == null) {
      actualWorkers = getAliveWorkers();
    }
    int[] workersArray = MyriaUtils.integerCollectionToIntArray(actualWorkers);

    /* The master plan: send the tuples out. */
    ExchangePairID scatterId = ExchangePairID.newID();
    ShuffleProducer scatter =
        new ShuffleProducer(source, scatterId, workersArray, new RoundRobinPartitionFunction(workersArray.length));

    /* The workers' plan */
    ShuffleConsumer gather = new ShuffleConsumer(source.getSchema(), scatterId, new int[] { MyriaConstants.MASTER_ID });
    SQLiteInsert insert = new SQLiteInsert(gather, relationKey, true);
    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    for (Integer workerId : workersArray) {
      workerPlans.put(workerId, new RootOperator[] { insert });
    }

    try {
      /* Start the workers */
      submitQuery("ingest " + relationKey.toString("sqlite"), "ingest " + relationKey.toString("sqlite"), scatter,
          workerPlans).sync();
      /* Now that the query has finished, add the metadata about this relation to the dataset. */
      catalog.addRelationMetadata(relationKey, source.getSchema());

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
   * @param configKey config key.
   * @return master configuration.
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

  /**
   * @return the socket info for the master.
   */
  protected SocketInfo getSocketInfo() {
    return masterSocketInfo;
  }
}