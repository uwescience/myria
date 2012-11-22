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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
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
  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = Logger.getLogger(Worker.class.getName());

  protected final class MessageProcessor extends Thread {
    @Override
    public void run() {

      TERMINATE_MESSAGE_PROCESSING : while (true) {
        MessageWrapper mw = null;
        try {
          mw = messageQueue.take();
        } catch (final InterruptedException e) {
          e.printStackTrace();
          break TERMINATE_MESSAGE_PROCESSING;
        }
        final TransportMessage m = mw.message;
        final Integer senderID = mw.senderID;

        switch (m.getType().getNumber()) {
          case TransportMessage.TransportMessageType.QUERY_VALUE:
            try {
              final ObjectInputStream osis =
                  new ObjectInputStream(new ByteArrayInputStream(m.getQuery().getQuery().toByteArray()));
              final Operator[] query = (Operator[]) (osis.readObject());
              try {
                receiveQuery(query);
              } catch (DbException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
              sendMessageToMaster(IPCUtils.CONTROL_QUERY_READY, null);
            } catch (IOException | ClassNotFoundException e) {
              e.printStackTrace();
            }
            break;
          case TransportMessage.TransportMessageType.DATA_VALUE:
            final DataMessage data = m.getData();
            final ExchangePairID exchangePairID = ExchangePairID.fromExisting(data.getOperatorID());
            final Schema operatorSchema = exchangeSchema.get(exchangePairID);
            switch (data.getType().getNumber()) {
              case DataMessageType.EOS_VALUE:
                receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema));
                break;
              case DataMessageType.NORMAL_VALUE:
                final List<ColumnMessage> columnMessages = data.getColumnsList();
                final Column<?>[] columnArray = new Column[columnMessages.size()];
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
          case TransportMessage.TransportMessageType.CONTROL_VALUE:
            final ControlMessage controlM = m.getControl();
            switch (controlM.getType().getNumber()) {
              case ControlMessage.ControlMessageType.SHUTDOWN_VALUE:
                System.out.println("shutdown requested");
                toShutdown = true;
                break;
              case ControlMessage.ControlMessageType.START_QUERY_VALUE:
                executeQuery();
                break;
            }
            break;
        }

      }

    }
  }

  public static class MessageWrapper {
    public MessageWrapper(final int senderID, final TransportMessage message) {
      this.senderID = senderID;
      this.message = message;
    }

    public int senderID;
    public TransportMessage message;
  }

  /**
   * The working thread, which executes the query plan.
   */
  protected class QueryExecutor extends Thread {
    @Override
    public final void run() {
      while (true) {
        Operator[] queries = null;
        queries = queryPlan;
        if (queries != null) {
          LOGGER.log(Level.INFO, "Worker start processing query");
          for (Operator query : queries) {
            try {
              ((Producer) query).open();
            } catch (final DbException e1) {
              e1.printStackTrace();
            }
          }
          int endCount = 0;
          while (true) {
            for (Operator query : queries) {
              try {
                if (query.isOpen() && ((Producer) query).next() == null) {
                  endCount++;
                  ((Producer) query).close();
                }
              } catch (final DbException e1) {
                e1.printStackTrace();
              }
            }
            if (endCount == queries.length) {
              break;
            }
          }
          finishQuery();
        }

        synchronized (queryExecutor) {
          try {
            // wait until a query plan is received
            queryExecutor.wait();
          } catch (final InterruptedException e) {
            break;
          }
        }
      }
    }
  }

  /**
   * The controller class which decides whether this worker should shutdown or not. 1) it detects whether the server is
   * still alive. If the server got killed because of any reason, the workers will be terminated. 2) it detects whether
   * a shutdown message is received.
   */
  protected class WorkerLivenessController {
    private final Timer timer = new Timer();

    protected class ShutdownChecker extends TimerTask {
      @Override
      public final void run() {
        if (toShutdown) {
          shutdown();
          // JVMUtils.shutdownVM();
        }

      }

    }

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
            System.out.println("Cannot connect the server: " + masterSocketInfo
                + " Maybe the server is down. I'll shutdown now.");
            System.out.println("Bye!");
            timer.cancel();
            JVMUtils.shutdownVM();
          } else {
            connectionPool.releaseLongTermConnection(serverChannel);
          }
        }
        inRun = false;
      }
    }

    public final void start() {
      try {
        timer.schedule(new ShutdownChecker(), 100, 100);
        timer.schedule(new Reporter(), (long) (Math.random() * 3000) + 5000, // initial
            // delay
            (long) (Math.random() * 2000) + 1000); // subsequent
        // rate
      } catch (final IllegalStateException e) {
        /* already got canceled, ignore. */
      }
    }
  }

  static final String usage = "Usage: worker [--conf <conf_dir>]";

  public static final String DEFAULT_DATA_DIR = "data";

  /**
   * Find out all the ParallelOperatorIDs of all consuming operators: ShuffleConsumer, CollectConsumer, and
   * BloomFilterConsumer running at this worker. The inBuffer needs the IDs to distribute the ExchangeMessages received.
   */
  public static void collectConsumerOperatorIDs(final Operator[] roots, final ArrayList<ExchangePairID> oIds) {
    for (Operator root : roots) {
      collectConsumerOperatorIDs(root, oIds);
    }
  }

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

  public static void main(String[] args) throws Throwable {
    if (args.length > 2) {
      LOGGER.log(Level.SEVERE, "Invalid number of arguments.\n" + usage);
      JVMUtils.shutdownVM();
    }

    // Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    // Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    String workingDir = null;
    if (args.length >= 2) {
      if (args[0].equals("--workingDir")) {
        workingDir = args[1];
        args = ParallelUtility.removeArg(args, 0);
        ParallelUtility.removeArg(args, 0);
      } else {
        LOGGER.log(Level.SEVERE, "Invalid arguments.\n" + usage);
        JVMUtils.shutdownVM();
      }
    }

    LOGGER.log(Level.INFO, "workingDir: " + workingDir);
    // Instantiate a new worker
    final Worker w = new Worker(workingDir);
    // int port = w.port;

    // Prepare to receive messages over the network
    w.init();
    // Start the actual message handler by binding
    // the acceptor to a network socket
    // Now the worker can accept messages
    w.start();

    // System.out.println("Worker started at:" + w.catalog.getWorkers().get(w.myID));
    LOGGER.log(Level.INFO, "Worker started at:" + w.catalog.getWorkers().get(w.myID));

    // From now on, the worker will listen for
    // messages to arrive on the network. These messages
    // will be handled by the WorkerHandler (see class WorkerHandler below,
    // in particular method messageReceived).

  }

  private final Properties databaseHandle;

  /**
   * The ID of this worker.
   */
  final int myID;

  /**
   * connectionPool[0] is always the master.
   */
  protected final IPCConnectionPool connectionPool;

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

  public final QueryExecutor queryExecutor;

  public final MessageProcessor messageProcessor;

  /**
   * The I/O buffer, all the ExchangeMessages sent to this worker are buffered here.
   */
  protected final HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>> dataBuffer;
  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  protected final LinkedBlockingQueue<MessageWrapper> messageQueue;

  protected final WorkerCatalog catalog;
  protected final SocketInfo masterSocketInfo;
  protected final SocketInfo mySocketInfo;

  public Worker(final String workingDirectory) throws CatalogException, FileNotFoundException {
    catalog = WorkerCatalog.open(FilenameUtils.concat(workingDirectory, "worker.catalog"));
    myID = Integer.parseInt(catalog.getConfigurationValue("worker.identifier"));
    databaseHandle = new Properties();
    String databaseType = catalog.getConfigurationValue("worker.data.type");
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
    mySocketInfo = catalog.getWorkers().get(myID);

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
   */
  public final void executeQuery() {
    LOGGER.log(Level.INFO, "Query started");
    synchronized (Worker.this.queryExecutor) {
      Worker.this.queryExecutor.notifyAll();
    }
  }

  /**
   * This method should be called when a query is finished.
   */
  public final void finishQuery() {
    if (queryPlan != null) {
      dataBuffer.clear();
      queryPlan = null;
    }
    LOGGER.log(Level.INFO, "My part of the query finished");
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
    for (Operator queryPlan : queryPlans) {
      localizeQueryPlan(queryPlan);
    }
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
      insert.setExecutorService(Executors.newSingleThreadExecutor());
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

    Operator[] children = queryPlan.getChildren();

    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          localizeQueryPlan(child);
        }
      }
    }
  }

  /**
   * This method should be called when a data item is received.
   */
  public final void receiveData(final ExchangeData data) {
    LOGGER.log(Level.INFO, "TupleBag received from " + data.getWorkerID() + " to Operator: " + data.getOperatorID());
    LinkedBlockingQueue<ExchangeData> q = null;
    q = Worker.this.dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeData) {
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
  public final void receiveQuery(final Operator[] queries) throws DbException {
    LOGGER.log(Level.INFO, "Query received");
    if (Worker.this.queryPlan != null) {
      LOGGER.log(Level.INFO, "Error: Worker is still processing. New query refused");
      return;
    }
    System.out.println("Query received: " + queries);

    final ArrayList<ExchangePairID> ids = new ArrayList<ExchangePairID>();
    collectConsumerOperatorIDs(queries, ids);
    Worker.this.dataBuffer.clear();
    Worker.this.exchangeSchema.clear();
    for (final ExchangePairID id : ids) {
      Worker.this.dataBuffer.put(id, new LinkedBlockingQueue<ExchangeData>());
    }
    Worker.this.localizeQueryPlan(queries);
    Worker.this.queryPlan = queries;
  }

  protected final void sendMessageToMaster(final TransportMessage message, final ChannelFutureListener callback) {
    LOGGER.log(Level.INFO, "send back query ready");
    ChannelFuture cf = Worker.this.connectionPool.sendShortMessage(0, message);
    if (callback != null) {
      cf.addListener(callback);
    }
  }

  /**
   * This method should be called whenever the system is going to shutdown.
   */
  public final void shutdown() {
    LOGGER.log(Level.INFO, "Shutdown requested. Please wait when cleaning up...");
    // ParallelUtility.shutdownIPC(ipcServerChannel, connectionPool);
    connectionPool.shutdown().awaitUninterruptibly();
    LOGGER.log(Level.INFO, "shutdown IPC completed");
    toShutdown = true;
  }

  public final void start() throws IOException {
    // ipcServerChannel = ipcServer.bind(mySocketInfo.getAddress());
    connectionPool.start();
    queryExecutor.start();
    messageProcessor.start();
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    new WorkerLivenessController().start();

  }

}
