package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.operator.BlockingDataReceiver;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage.ControlMessageType;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Workers do the real query execution. A query received by the server will be pre-processed and then dispatched to the
 * workers.
 * 
 * To execute a query on a worker, 4 steps are proceeded:
 * 
 * 1) A worker receive a DbIterator instance as its execution plan. The worker then stores the plan and does some
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
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

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
              final Operator query = (Operator) (osis.readObject());
              try {
                receiveQuery(query);
              } catch (DbException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
              sendMessageToMaster(TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
                  ControlMessage.newBuilder().setType(ControlMessageType.QUERY_READY_TO_EXECUTE).build()).build(), null);
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
                receiveData(new ExchangeTupleBatch(exchangePairID, senderID, operatorSchema));
                break;
              case DataMessageType.NORMAL_VALUE:
                final List<ColumnMessage> columnMessages = data.getColumnsList();
                final Column<?>[] columnArray = new Column[columnMessages.size()];
                int idx = 0;
                for (final ColumnMessage cm : columnMessages) {
                  columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
                }
                final List<Column<?>> columns = Arrays.asList(columnArray);
                receiveData(new ExchangeTupleBatch(exchangePairID, senderID, columns, operatorSchema, columnMessages
                    .get(0).getNumTuples()));

                break;
            }
            break;
          case TransportMessage.TransportMessageType.CONTROL_VALUE:
            final ControlMessage controlM = m.getControl();
            switch (controlM.getType().getNumber()) {
              case ControlMessage.ControlMessageType.SHUTDOWN_VALUE:
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
        Operator query = null;
        query = queryPlan;
        if (query != null) {
          LOGGER.debug("Worker start processing query");
          final CollectProducer root = (CollectProducer) query;
          try {
            root.open();
            while (root.next() != null) {
            }
            root.close();
          } catch (final DbException e1) {
            e1.printStackTrace();
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
          ParallelUtility.shutdownVM();
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
          serverChannel = connectionPool.get(0, 3, null);
        } catch (final RuntimeException e) {
          e.printStackTrace();
        } catch (final Exception e) {
          e.printStackTrace();
        }

        if (serverChannel == null) {
          System.out.println("Cannot connect the server: " + masterSocketInfo
              + " Maybe the server is down. I'll shutdown now.");
          System.out.println("Bye!");
          timer.cancel();
          ParallelUtility.shutdownVM();
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
      LOGGER.error("Invalid number of arguments.\n" + usage);
      ParallelUtility.shutdownVM();
    }

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    String workingDir = null;
    if (args.length >= 2) {
      if (args[0].equals("--workingDir")) {
        workingDir = args[1];
        args = ParallelUtility.removeArg(args, 0);
        ParallelUtility.removeArg(args, 0);
      } else {
        LOGGER.error("Invalid arguments.\n" + usage);
        ParallelUtility.shutdownVM();
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

    LOGGER.debug("Worker started at:" + w.catalog.getWorkers().get(w.myID));

    // From now on, the worker will listen for
    // messages to arrive on the network. These messages
    // will be handled by the WorkerHandler (see class WorkerHandler below,
    // in particular method messageReceived).

  }

  private final File dataDir;

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
  final ServerBootstrap ipcServer;
  private Channel ipcServerChannel;

  /**
   * The current query plan.
   */
  private volatile Operator queryPlan = null;

  /**
   * A indicator of shutting down the worker.
   */
  private volatile boolean toShutdown = false;

  public final QueryExecutor queryExecutor;

  public final MessageProcessor messageProcessor;

  /**
   * The I/O buffer, all the ExchangeMessages sent to this worker are buffered here.
   */
  protected final HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>> dataBuffer;
  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  protected final LinkedBlockingQueue<MessageWrapper> messageQueue;

  protected final WorkerCatalog catalog;
  protected final SocketInfo masterSocketInfo;
  protected final SocketInfo mySocketInfo;

  public Worker(final String workingDirectory) throws CatalogException, FileNotFoundException {
    catalog = WorkerCatalog.open(FilenameUtils.concat(workingDirectory, "worker.catalog"));
    myID = Integer.parseInt(catalog.getConfigurationValue("worker.identifier"));
    dataDir = new File(catalog.getConfigurationValue("worker.data.sqlite.dir"));
    mySocketInfo = catalog.getWorkers().get(myID);

    dataBuffer = new HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>>();
    messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    ipcServer = ParallelUtility.createWorkerIPCServer(messageQueue);
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
  }

  /**
   * execute the current query, note that this method is invoked by the Mina IOHandler thread. Typically, IOHandlers
   * should focus on accepting/routing IO requests, rather than do heavily loaded work.
   */
  public final void executeQuery() {
    LOGGER.debug("Query started");
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
    LOGGER.debug("My part of the query finished");
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
  public final void localizeQueryPlan(final Operator queryPlan) throws DbException {
    if (queryPlan == null) {
      return;
    }

    if (queryPlan instanceof SQLiteQueryScan) {
      final SQLiteQueryScan ss = ((SQLiteQueryScan) queryPlan);
      ss.setDataDir(dataDir.getAbsolutePath());
    } else if (queryPlan instanceof SQLiteSQLProcessor) {
      final SQLiteSQLProcessor ss = ((SQLiteSQLProcessor) queryPlan);
      ss.setDataDir(dataDir.getAbsolutePath());
    } else if (queryPlan instanceof Producer) {
      ((Producer) queryPlan).setThisWorker(Worker.this);
    } else if (queryPlan instanceof Consumer) {
      final Consumer c = (Consumer) queryPlan;

      LinkedBlockingQueue<ExchangeTupleBatch> buf = null;
      buf = Worker.this.dataBuffer.get(((Consumer) queryPlan).getOperatorID());
      c.setInputBuffer(buf);
      exchangeSchema.put(c.getOperatorID(), c.getSchema());

    } else if (queryPlan instanceof BlockingDataReceiver) {
      final BlockingDataReceiver bdr = (BlockingDataReceiver) queryPlan;
      final _TupleBatch outputBuffer = bdr.getOutputBuffer();
      if (outputBuffer instanceof SQLiteTupleBatch) {
        ((SQLiteTupleBatch) outputBuffer).reset(dataDir.getAbsolutePath());
      }
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
  public final void receiveData(final ExchangeTupleBatch data) {
    if (data instanceof _TupleBatch) {
      LOGGER.debug("TupleBag received from " + data.getWorkerID() + " to Operator: " + data.getOperatorID());
    }
    LinkedBlockingQueue<ExchangeTupleBatch> q = null;
    q = Worker.this.dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeTupleBatch) {
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
  public final void receiveQuery(final Operator query) throws DbException {
    LOGGER.debug("Query received");
    if (Worker.this.queryPlan != null) {
      LOGGER.error("Error: Worker is still processing. New query refused");
      return;
    }

    final ArrayList<ExchangePairID> ids = new ArrayList<ExchangePairID>();
    collectConsumerOperatorIDs(query, ids);
    Worker.this.dataBuffer.clear();
    Worker.this.exchangeSchema.clear();
    for (final ExchangePairID id : ids) {
      Worker.this.dataBuffer.put(id, new LinkedBlockingQueue<ExchangeTupleBatch>());
    }
    Worker.this.localizeQueryPlan(query);
    Worker.this.queryPlan = query;
  }

  protected final void sendMessageToMaster(final TransportMessage message, final ChannelFutureListener callback) {
    LOGGER.debug("send back query ready");
    Channel s = null;
    s = Worker.this.connectionPool.get(0, 3, null);
    if (callback != null) {
      s.write(message).addListener(callback);
    } else {
      s.write(message);
    }
  }

  /**
   * This method should be called whenever the system is going to shutdown.
   */
  public final void shutdown() {
    LOGGER.debug("Shutdown requested. Please wait when cleaning up...");
    ParallelUtility.shutdownIPC(ipcServerChannel, connectionPool);
    LOGGER.debug("shutdown IPC completed");
    toShutdown = true;
  }

  public final void start() throws IOException {
    ipcServerChannel = ipcServer.bind(mySocketInfo.getAddress());
    queryExecutor.start();
    messageProcessor.start();
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    new WorkerLivenessController().start();

  }

}
