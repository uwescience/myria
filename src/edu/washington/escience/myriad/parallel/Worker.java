package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.operator.BlockingSQLiteDataReceiver;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
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
public class Worker {
  /** A short amount of time to sleep waiting for network events. */
  public static final int SHORT_SLEEP_MILLIS = 100;

  protected final class MessageProcessor extends Thread {

    /** Constructor, set the thread name. */
    public MessageProcessor() {
      super();
      setName("Worker-MessageProcessor-" + getId());
    }

    /** Whether this thread should stop. */
    private volatile boolean stopped = false;

    @Override
    public void run() {

      TERMINATE_MESSAGE_PROCESSING : while (!stopped) {
        MessageWrapper mw = null;
        try {
          final int pollDelay = 100;
          mw = messageQueue.poll(pollDelay, TimeUnit.MILLISECONDS);
          if (mw == null) {
            continue;
          }
        } catch (final InterruptedException e) {
          e.printStackTrace();
          break TERMINATE_MESSAGE_PROCESSING;
        }
        final TransportMessage m = mw.message;
        final Integer senderID = mw.senderID;

        switch (m.getType()) {
          case QUERY:
            try {
              final long queryId = m.getQuery().getQueryId();
              final ObjectInputStream osis =
                  new ObjectInputStream(new ByteArrayInputStream(m.getQuery().getQuery().toByteArray()));
              final Operator[] query = (Operator[]) (osis.readObject());
              try {
                receiveQuery(queryId, query);
              } catch (final DbException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
              sendMessageToMaster(IPCUtils.queryReadyTM(myID, queryId), null);
            } catch (IOException | ClassNotFoundException e) {
              e.printStackTrace();
            }
            break;
          case DATA:
            final DataMessage data = m.getData();
            final ExchangePairID exchangePairID = ExchangePairID.fromExisting(data.getOperatorID());
            final Schema operatorSchema = exchangeSchema.get(exchangePairID);

            switch (data.getType()) {
              case EOS:
                receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, 0));
                break;
              case EOI:
                receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, 1));
                break;
              case NORMAL:
                final List<ColumnMessage> columnMessages = data.getColumnsList();
                final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
                int idx = 0;
                for (final ColumnMessage cm : columnMessages) {
                  columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
                }
                final List<Column<?>> columns = Arrays.asList(columnArray);
                receiveData(new ExchangeData(exchangePairID, senderID, columns, operatorSchema, columnMessages.get(0)
                    .getNumTuples()));

                break;
            }
            break;
          case CONTROL:
            final ControlMessage controlM = m.getControl();
            switch (controlM.getType()) {
              case SHUTDOWN:
                LOGGER.info("shutdown requested");
                toShutdown = true;
                break;
              case START_QUERY:
                Long queryId = controlM.getQueryId();
                executeQuery(queryId);
                break;
              case DISCONNECT:
                /* TODO */
                break;
              case CONNECT:
              case QUERY_COMPLETE:
              case QUERY_READY_TO_EXECUTE:
              case WORKER_ALIVE:
                throw new RuntimeException("Unexpected control message received at worker: " + controlM.getType());
            }
            break;
        }

      }

    }

    /** Stop this thread. */
    public void setStopped() {
      stopped = true;
    }
  }

  public static class MessageWrapper {
    public int senderID;

    public TransportMessage message;

    public MessageWrapper(final int senderID, final TransportMessage message) {
      this.senderID = senderID;
      this.message = message;
    }
  }

  /**
   * The working thread, which executes the query plan.
   */
  protected class QueryExecutor extends Thread {
    /** Constructor, set the thread name. */
    public QueryExecutor() {
      super();
      setName("Worker-QueryExecutor-" + getId());
    }

    /** Whether this worker has been stopped. */
    private volatile boolean stopped = false;

    @Override
    public final void run() {
      while (!stopped) {
        Operator[] queries = queryPlan;
        long queryId = myQueryId;
        if (queries != null && startedQueryId >= myQueryId) {
          LOGGER.info("Worker start processing query " + queryId);
          for (final Operator query : queries) {
            try {
              query.open();
            } catch (final DbException e) {
              throw new RuntimeException(e);
            }
          }
          int endCount = 0;
          while (true) {
            for (final Operator query : queries) {
              try {
                if (query.isOpen() && query.next() == null) {
                  endCount++;
                  query.close();
                }
              } catch (final DbException e) {
                throw new RuntimeException(e);
              }
            }
            if (endCount == queries.length) {
              break;
            }
          }
          finishQuery(queryId);
        }

        synchronized (queryExecutor) {
          try {
            // wait until a query plan is received
            queryExecutor.wait(SHORT_SLEEP_MILLIS);
          } catch (final InterruptedException e) {
            break;
          }
        }
      }
    }

    /** Stop this queryExecutor after the query finishes. */
    public final void setStopped() {
      stopped = true;
    }
  }

  /**
   * The controller class which decides whether this worker should shutdown or not. 1) it detects whether the server is
   * still alive. If the server got killed because of any reason, the workers will be terminated. 2) it detects whether
   * a shutdown message is received.
   */
  protected class WorkerLivenessController {
    protected class Reporter extends TimerTask {
      private volatile boolean inRun = false;

      @Override
      public final void run() {
        if (inRun) {
          return;
        }
        inRun = true;
        Channel serverChannel = null;
        try {
          serverChannel = connectionPool.reserveLongTermConnection(0);
        } catch (final RuntimeException e) {
          e.printStackTrace();
        } catch (final Exception e) {
          e.printStackTrace();
        } finally {
          if (serverChannel == null) {
            LOGGER.info("Cannot connect the server: " + masterSocketInfo
                + " Maybe the server is down. I'll shutdown now.");
            LOGGER.info("Bye!");
            timer.cancel();
            JVMUtils.shutdownVM();
          } else {
            connectionPool.releaseLongTermConnection(serverChannel);
          }
        }
        inRun = false;
      }
    }

    protected class ShutdownChecker extends TimerTask {
      @Override
      public final void run() {
        if (toShutdown) {
          shutdown();
          timer.cancel();
          // JVMUtils.shutdownVM();
        }

      }

    }

    private final Timer timer = new Timer();

    public final void start() {
      try {
        timer.schedule(new ShutdownChecker(), 100, SHORT_SLEEP_MILLIS);
        timer.schedule(new Reporter(), (long) (Math.random() * 3000) + 5000, (long) (Math.random() * 2000) + 1000);
      } catch (final IllegalStateException e) {
        /* already got canceled, ignore. */
        assert true; /* Do nothing. */
      }
    }
  }

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Worker.class);

  static final String USAGE = "Usage: worker [--conf <conf_dir>]";

  public static final String DEFAULT_DATA_DIR = "data";

  public static void collectConsumerOperatorIDs(final Operator root, final ArrayList<ExchangePairID> oIds) {
    if (root instanceof Consumer) {
      oIds.add(((Consumer) root).getOperatorID());
    }
    final Operator[] ops = root.getChildren();
    if (ops != null) {
      for (final Operator c : ops) {
        if (c != null) {
          collectConsumerOperatorIDs(c, oIds);
        } else {
          return;
        }
      }
    }
  }

  /**
   * Find out all the ParallelOperatorIDs of all consuming operators: ShuffleConsumer, CollectConsumer, and
   * BloomFilterConsumer running at this worker. The inBuffer needs the IDs to distribute the ExchangeMessages received.
   */
  public static void collectConsumerOperatorIDs(final Operator[] roots, final ArrayList<ExchangePairID> oIds) {
    for (final Operator root : roots) {
      collectConsumerOperatorIDs(root, oIds);
    }
  }

  public static void main(String[] args) throws Throwable {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    if (args.length > 2) {
      LOGGER.warn("Invalid number of arguments.\n" + USAGE);
      JVMUtils.shutdownVM();
    }

    String workingDir = null;
    if (args.length >= 2) {
      if (args[0].equals("--workingDir")) {
        workingDir = args[1];
        args = ParallelUtility.removeArg(args, 0);
        ParallelUtility.removeArg(args, 0);
      } else {
        LOGGER.warn("Invalid arguments.\n" + USAGE);
        JVMUtils.shutdownVM();
      }
    }

    LOGGER.debug("workingDir: " + workingDir);
    // Instantiate a new worker
    final Worker w = new Worker(workingDir);
    // int port = w.port;

    // Prepare to receive messages over the network
    w.init();
    // Start the actual message handler by binding
    // the acceptor to a network socket
    // Now the worker can accept messages
    w.start();

    LOGGER.info("Worker started at:" + w.catalog.getWorkers().get(w.myID));

    // From now on, the worker will listen for
    // messages to arrive on the network. These messages
    // will be handled by the WorkerHandler (see class WorkerHandler below,
    // in particular method messageReceived).

  }

  private final Properties databaseHandle;

  /**
   * The ID of this worker.
   */
  private final int myID;

  /** The ID of the currently running query. */
  private volatile long myQueryId;

  /** The ID of the highest started query. */
  private volatile long startedQueryId;

  /**
   * connectionPool[0] is always the master.
   */
  private final IPCConnectionPool connectionPool;

  /**
   * The acceptor, which binds to a TCP socket and waits for connections
   * 
   * The Server sends messages, including query plans and control messages, to the worker through this acceptor.
   * 
   * Other workers send tuples during query execution to this worker also through this acceptor.
   */
  // final ServerBootstrap ipcServer;
  // private Channel ipcServerChannel;

  /**
   * The current query plan.
   */
  private volatile Operator[] queryPlan = null;

  /**
   * A indicator of shutting down the worker.
   */
  private volatile boolean toShutdown = false;

  private final QueryExecutor queryExecutor;

  private final MessageProcessor messageProcessor;

  /**
   * The I/O buffer, all the ExchangeMessages sent to this worker are buffered here.
   */
  private final HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>> dataBuffer;
  private final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  private final LinkedBlockingQueue<MessageWrapper> messageQueue;

  private final WorkerCatalog catalog;
  private final SocketInfo masterSocketInfo;

  public Worker(final String workingDirectory) throws CatalogException, FileNotFoundException {
    catalog = WorkerCatalog.open(FilenameUtils.concat(workingDirectory, "worker.catalog"));
    myID = Integer.parseInt(catalog.getConfigurationValue("worker.identifier"));
    startedQueryId = -1;
    databaseHandle = new Properties();
    final String databaseType = catalog.getConfigurationValue("worker.data.type");
    switch (databaseType) {
      case "sqlite":
        databaseHandle.setProperty("sqliteFile", catalog.getConfigurationValue("worker.data.sqlite.db"));
        break;
      case "mysql":
        /* TODO fill this in. */
        break;
      default:
        throw new CatalogException("Unknown worker type: " + databaseType);
    }

    dataBuffer = new HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>>();
    messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    queryExecutor = new QueryExecutor();
    queryExecutor.setDaemon(true);
    messageProcessor = new MessageProcessor();
    messageProcessor.setDaemon(false);
    masterSocketInfo = catalog.getMasters().get(0);

    final Map<Integer, SocketInfo> workers = catalog.getWorkers();
    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, masterSocketInfo);

    connectionPool = new IPCConnectionPool(myID, computingUnits, messageQueue);
    // ipcServer = ParallelUtility.createWorkerIPCServer(messageQueue, connectionPool);
  }

  /**
   * execute the current query, note that this method is invoked by the Mina IOHandler thread. Typically, IOHandlers
   * should focus on accepting/routing IO requests, rather than do heavily loaded work.
   * 
   * @param queryId the query being started.
   */
  public final void executeQuery(final Long queryId) {
    LOGGER.info("Query " + queryId + " started");
    startedQueryId = queryId;
    synchronized (queryExecutor) {
      queryExecutor.notifyAll();
    }
  }

  /**
   * This method should be called when a query is finished.
   * 
   * @param queryId the ID of the query that just finished.
   */
  public final void finishQuery(final long queryId) {
    if (queryPlan != null) {
      dataBuffer.clear();
      queryPlan = null;
    }
    sendMessageToMaster(IPCUtils.queryCompleteTM(myID, queryId), null);
    LOGGER.info("My part of query " + queryId + " finished");
  }

  /**
   * Initialize.
   */
  public final void init() throws IOException {
  }

  /**
   * Return true if the worker is now executing a query.
   */
  public final boolean isRunning() {
    return queryPlan != null;
  }

  public final void localizeQueryPlan(final Operator queryPlan) throws DbException {
    if (queryPlan == null) {
      return;
    }
    if (queryPlan instanceof SQLiteQueryScan) {
      final String sqliteDatabaseFilename = databaseHandle.getProperty("sqliteFile");
      if (sqliteDatabaseFilename == null) {
        throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
      }
      final SQLiteQueryScan ss = ((SQLiteQueryScan) queryPlan);
      ss.setPathToSQLiteDb(sqliteDatabaseFilename);
    } else if (queryPlan instanceof SQLiteSQLProcessor) {
      final String sqliteDatabaseFilename = databaseHandle.getProperty("sqliteFile");
      if (sqliteDatabaseFilename == null) {
        throw new DbException("Unable to instantiate SQLiteSQLProcessor on non-sqlite worker");
      }
      final SQLiteSQLProcessor ss = ((SQLiteSQLProcessor) queryPlan);
      ss.setPathToSQLiteDb(sqliteDatabaseFilename);
    } else if (queryPlan instanceof SQLiteInsert) {
      final String sqliteDatabaseFilename = databaseHandle.getProperty("sqliteFile");
      if (sqliteDatabaseFilename == null) {
        throw new DbException("Unable to instantiate SQLiteInsert on non-sqlite worker");
      }
      final SQLiteInsert insert = ((SQLiteInsert) queryPlan);
      insert.setPathToSQLiteDb(sqliteDatabaseFilename);
    } else if (queryPlan instanceof Producer) {
      ((Producer) queryPlan).setConnectionPool(connectionPool);
    } else if (queryPlan instanceof Consumer) {
      final Consumer c = (Consumer) queryPlan;

      LinkedBlockingQueue<ExchangeData> buf = null;
      buf = Worker.this.dataBuffer.get(((Consumer) queryPlan).getOperatorID());
      c.setInputBuffer(buf);
      exchangeSchema.put(c.getOperatorID(), c.getSchema());

    } else if (queryPlan instanceof BlockingSQLiteDataReceiver) {
      final String sqliteDatabaseFilename = databaseHandle.getProperty("sqliteFile");
      if (sqliteDatabaseFilename == null) {
        throw new DbException("Unable to instantiate BlockingSQLiteDataReceiver on non-sqlite worker");
      }
      final BlockingSQLiteDataReceiver bdr = (BlockingSQLiteDataReceiver) queryPlan;
      bdr.setPathToSQLiteDb(sqliteDatabaseFilename);
    }

    final Operator[] children = queryPlan.getChildren();

    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          localizeQueryPlan(child);
        }
      }
    }
  }

  /**
   * localize the received query plan. Some information that are required to get the query plan executed need to be
   * replaced by local versions. For example, the table in the SeqScan operator need to be replaced by the local table.
   * Note that Producers and Consumers also needs local information.
   * 
   * @throws DbException
   */
  public final void localizeQueryPlan(final Operator[] queryPlans) throws DbException {
    if (queryPlans == null) {
      return;
    }
    for (final Operator queryPlan : queryPlans) {
      localizeQueryPlan(queryPlan);
    }
  }

  /**
   * This method should be called when a data item is received.
   */
  public final void receiveData(final ExchangeData data) {
    LOGGER.debug("TupleBag received from " + data.getWorkerID() + " to Operator: " + data.getOperatorID());
    LinkedBlockingQueue<ExchangeData> q = null;
    q = Worker.this.dataBuffer.get(data.getOperatorID());
    if (q != null) {
      // if q == null it means dataBuffer has been cleaned by finishQuery()
      q.offer(data);
    }
  }

  /**
   * this method should be called when a query is received from the server.
   * 
   * It does the initialization and preparation for the execution of the query.
   * 
   * @throws DbException
   */
  public final void receiveQuery(final Long queryId, final Operator[] queries) throws DbException {
    LOGGER.info("Query " + queryId + " received");
    if (queryPlan != null) {
      LOGGER.info("Error: Worker is still processing " + myQueryId + ". New query " + queryId + " refused");
      return;
    }

    final ArrayList<ExchangePairID> ids = new ArrayList<ExchangePairID>();
    collectConsumerOperatorIDs(queries, ids);
    dataBuffer.clear();
    exchangeSchema.clear();
    for (final ExchangePairID id : ids) {
      dataBuffer.put(id, new LinkedBlockingQueue<ExchangeData>());
    }
    localizeQueryPlan(queries);
    queryPlan = queries;
    myQueryId = queryId;
  }

  protected final void sendMessageToMaster(final TransportMessage message, final ChannelFutureListener callback) {
    final ChannelFuture cf = Worker.this.connectionPool.sendShortMessage(0, message);
    if (callback != null) {
      cf.addListener(callback);
    }
  }

  /**
   * This method should be called whenever the system is going to shutdown.
   */
  public final void shutdown() {
    LOGGER.info("Shutdown requested. Please wait when cleaning up...");
    connectionPool.shutdown().awaitUninterruptibly();
    queryExecutor.setStopped();
    messageProcessor.setStopped();
    LOGGER.info("shutdown IPC completed");
    toShutdown = false;
  }

  public final void start() throws IOException {
    connectionPool.start();
    queryExecutor.start();
    messageProcessor.start();
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    new WorkerLivenessController().start();
    /* Tell the master we're alive. */
    sendMessageToMaster(IPCUtils.CONTROL_WORKER_ALIVE, null);
  }
}
