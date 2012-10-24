package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayInputStream;
import java.io.File;
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

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
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

  protected class MessageProcessor extends Thread {
    @Override
    public final void run() {

      TERMINATE_MESSAGE_PROCESSING : while (true) {
        MessageWrapper mw = null;
        try {
          mw = messageBuffer.take();
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
              receiveQuery(query);
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
                final Column[] columnArray = new Column[columnMessages.size()];
                int idx = 0;
                for (final ColumnMessage cm : columnMessages) {
                  columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
                }
                final List<Column> columns = Arrays.asList(columnArray);
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
    protected int senderID;
    protected TransportMessage message;
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
          System.out.println("Worker start processing query");
          final CollectProducer root = (CollectProducer) query;
          try {
            root.open();
            while (root.hasNext()) {
              root.next();
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
   * Mina acceptor handler. This is where all messages arriving from other workers and from the coordinator will be
   * processed
   */
  public class WorkerHandler extends IoHandlerAdapter {

    @Override
    public final void exceptionCaught(final IoSession session, final Throwable cause) {
      System.out.println("exception caught");
      cause.printStackTrace();
      ParallelUtility.closeSession(session);
    }

    /**
     * Got called everytime a message is received.
     */
    @Override
    public final void messageReceived(final IoSession session, final Object message) {
      if (message instanceof TransportMessage) {
        final TransportMessage tm = (TransportMessage) message;
        if ((tm.getType() == TransportMessage.TransportMessageType.CONTROL)
            && (ControlMessage.ControlMessageType.CONNECT == tm.getControl().getType())) {
          // connect request sent from other workers
          final ControlMessage cm = tm.getControl();
          // ControlProto.ExchangePairID epID = cm.getExchangePairID();

          session.setAttribute("remoteID", cm.getRemoteID());
        } else {
          final Integer senderID = (Integer) session.getAttribute("remoteID");
          if (senderID != null) {
            final MessageWrapper mw = new MessageWrapper();
            mw.senderID = senderID;
            mw.message = (TransportMessage) message;
            messageBuffer.add(mw);
          } else {
            // currently messages from the master do not have id
            // messages from master

            final MessageWrapper mw = new MessageWrapper();
            mw.senderID = 0;
            mw.message = (TransportMessage) message;
            messageBuffer.add(mw);
          }
        }
      } else {
        System.err.println("Error: Unknown message received: " + message);
      }

    }
  }

  /**
   * The controller class which decides whether this worker should shutdown or not. 1) it detects whether the server is
   * still alive. If the server got killed because of any reason, the workers will be terminated. 2) it detects whether
   * a shutdown message is received.
   */
  protected class WorkerLivenessController extends TimerTask {
    private final Timer timer = new Timer();
    private volatile boolean inRun = false;

    @Override
    public final void run() {
      if (toShutdown) {
        shutdown();
        ParallelUtility.shutdownVM();
      }
      if (inRun) {
        return;
      }
      inRun = true;
      IoSession serverSession = null;
      try {
        serverSession = connectionPool.get(0, null, 3, null);
      } catch (final RuntimeException e) {
        e.printStackTrace();
      } catch (final Exception e) {
        e.printStackTrace();
      }

      if (serverSession == null) {
        System.out.println("Cannot connect the server: " + conf.getServer()
            + " Maybe the server is down. I'll shutdown now.");
        System.out.println("Bye!");
        timer.cancel();
        ParallelUtility.shutdownVM();
      }
      ParallelUtility.closeSession(serverSession);
      inRun = false;
    }

    public final void start() {
      try {
        timer.schedule(this, (long) (Math.random() * 3000) + 5000, // initial
                                                                   // delay
            (long) (Math.random() * 2000) + 1000); // subsequent
                                                   // rate
      } catch (final IllegalStateException e) {
        /* already get canceled, ignore */
        /* Do nothing */;
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
      System.out.println("Invalid number of arguments.\n" + usage);
      ParallelUtility.shutdownVM();
    }

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);
    // Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.OFF);

    File confDir = null;
    if (args.length >= 2) {
      if (args[0].equals("--conf")) {
        confDir = new File(args[1]);
        args = ParallelUtility.removeArg(args, 0);
        ParallelUtility.removeArg(args, 0);
      } else {
        System.out.println("Invalid arguments.\n" + usage);
        ParallelUtility.shutdownVM();
      }
    }

    System.out.println("confdir: " + confDir.getAbsolutePath());
    final Configuration conf = new Configuration(confDir);
    // Instantiate a new worker
    final Worker w = new Worker(conf);
    // int port = w.port;

    // Prepare to receive messages over the network
    w.init();
    // Start the actual message handler by binding
    // the acceptor to a network socket
    // Now the worker can accept messages
    w.start();

    System.out.println("Worker started at:" + w.conf.getWorkers().get(w.myID));

    // From now on, the worker will listen for
    // messages to arrive on the network. These messages
    // will be handled by the WorkerHandler (see class WorkerHandler below,
    // in particular method messageReceived).

  }

  public final File dataDir;

  public final File tmpDir;

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
  final NioSocketAcceptor acceptor;

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
   * The IoHandler for network communication. The methods of this handler are called when some communication events
   * happen. For example a message is received, a new connection is created, etc.
   */
  final WorkerHandler minaHandler;

  /**
   * The I/O buffer, all the ExchangeMessages sent to this worker are buffered here.
   */
  protected final HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>> dataBuffer;
  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  protected final LinkedBlockingQueue<MessageWrapper> messageBuffer;

  protected final Configuration conf;

  public Worker(final Configuration conf) {
    this.conf = conf;
    myID = Integer.parseInt(conf.get("worker.identifier"));

    dataDir = new File(conf.get("worker.data.sqlite.dir"));
    tmpDir = new File(conf.get("worker.tmp.dir"));

    acceptor = ParallelUtility.createAcceptor();

    dataBuffer = new HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>>();
    messageBuffer = new LinkedBlockingQueue<MessageWrapper>();
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    queryExecutor = new QueryExecutor();
    queryExecutor.setDaemon(true);
    messageProcessor = new MessageProcessor();
    messageProcessor.setDaemon(false);
    minaHandler = new WorkerHandler();

    final Map<Integer, SocketInfo> workers = conf.getWorkers();
    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, conf.getServer());

    final Map<Integer, IoHandler> handlers = new HashMap<Integer, IoHandler>(workers.size() + 1);
    for (final Integer workerID : workers.keySet()) {
      handlers.put(workerID, minaHandler);
    }

    handlers.put(0, minaHandler);

    connectionPool = new IPCConnectionPool(myID, computingUnits, handlers);
  }

  /**
   * execute the current query, note that this method is invoked by the Mina IOHandler thread. Typically, IOHandlers
   * should focus on accepting/routing IO requests, rather than do heavily loaded work.
   */
  public final void executeQuery() {
    System.out.println("Query started");
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
    System.out.println("My part of the query finished");
  }

  /**
   * Initialize.
   */
  public final void init() throws IOException {
    acceptor.setHandler(minaHandler);
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
   */
  public final void localizeQueryPlan(final Operator queryPlan) {
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
      System.out.println("TupleBag received from " + data.getWorkerID() + " to Operator: " + data.getOperatorID());
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
   */
  public final void receiveQuery(final Operator query) {
    System.out.println("Query received");
    if (Worker.this.queryPlan != null) {
      System.err.println("Error: Worker is still processing. New query refused");
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

  protected final void sendMessageToMaster(final TransportMessage message, final IoFutureListener<?> callback) {
    System.out.println("send back query ready");
    IoSession s = null;
    s = Worker.this.connectionPool.get(0, null, 3, null);
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
    System.out.println("Shutdown requested. Please wait when cleaning up...");
    ParallelUtility.unbind(acceptor);
    toShutdown = true;
  }

  public final void start() throws IOException {
    acceptor.bind(conf.getWorkers().get(myID).getAddress());
    queryExecutor.start();
    messageProcessor.start();
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    new WorkerLivenessController().start();

  }

}
