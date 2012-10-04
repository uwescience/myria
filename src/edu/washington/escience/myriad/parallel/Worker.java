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

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
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
 * */
public class Worker {

  static final String usage = "Usage: worker [--conf <conf_dir>]";
  public final static String DEFAULT_DATA_DIR = "data";

  public final File dataDir;
  public final File tmpDir;

  /**
   * The working thread, which executes the query plan
   * */
  protected class QueryExecutor extends Thread {
    @Override
    public void run() {
      while (true) {
        Operator query = null;
        // synchronized (Worker.this) {
        query = Worker.this.queryPlan;
        // }
        if (query != null) {
          System.out.println("Worker start processing query");
          CollectProducer root = (CollectProducer) query;
          try {
            root.open();
            while (root.hasNext())
              root.next();
            root.close();
            // root.join(); // wait until the query to finish
          } catch (DbException e1) {
            e1.printStackTrace();
          }
          Worker.this.finishQuery();
        }

        synchronized (Worker.this.queryExecutor) {
          try {
            // wait until a query plan is received
            Worker.this.queryExecutor.wait();
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    }
  }

  protected class MessageProcessor extends Thread {
    @Override
    public void run() {

      TERMINATE_MESSAGE_PROCESSING : while (true) {
        MessageWrapper mw = null;
        try {
          mw = Worker.this.messageBuffer.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          break TERMINATE_MESSAGE_PROCESSING;
        }
        TransportMessage m = mw.message;
        Integer senderID = mw.senderID;

        switch (m.getType().getNumber()) {
          case TransportMessage.TransportMessageType.QUERY_VALUE:
            try {
              ObjectInputStream osis =
                  new ObjectInputStream(new ByteArrayInputStream(m.getQuery().getQuery().toByteArray()));
              Operator query = (Operator) (osis.readObject());
              // System.out.println(query);
              Worker.this.receiveQuery(query);
              Worker.this.sendMessageToMaster(TransportMessage.newBuilder().setType(TransportMessageType.CONTROL)
                  .setControl(ControlMessage.newBuilder().setType(ControlMessageType.QUERY_READY_TO_EXECUTE).build())
                  .build(), null);
            } catch (IOException | ClassNotFoundException e) {
              e.printStackTrace();
            }
            break;
          case TransportMessage.TransportMessageType.DATA_VALUE: {
            DataMessage data = m.getData();
            ExchangePairID exchangePairID = ExchangePairID.fromExisting(data.getOperatorID());
            Schema operatorSchema = Worker.this.exchangeSchema.get(exchangePairID);
            switch (data.getType().getNumber()) {
              case DataMessageType.EOS_VALUE:
                Worker.this.receiveData(new ExchangeTupleBatch(exchangePairID, senderID, operatorSchema));
                break;
              case DataMessageType.NORMAL_VALUE:
                List<ColumnMessage> columnMessages = data.getColumnsList();
                Column[] columnArray = new Column[columnMessages.size()];
                int idx = 0;
                for (ColumnMessage cm : columnMessages) {
                  columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
                }
                List<Column> columns = Arrays.asList(columnArray);
                Worker.this.receiveData(new ExchangeTupleBatch(exchangePairID, senderID, columns, operatorSchema,
                    columnMessages.get(0).getNumTuples()));

                break;
            }
          }
            break;
          case TransportMessage.TransportMessageType.CONTROL_VALUE:
            ControlMessage controlM = m.getControl();
            switch (controlM.getType().getNumber()) {
              case ControlMessage.ControlMessageType.SHUTDOWN_VALUE:
                Worker.this.toShutdown = true;
                break;
              case ControlMessage.ControlMessageType.START_QUERY_VALUE:
                Worker.this.executeQuery();
                break;
            }
            break;
        }

      }

    }
  }

  /**
   * The controller class which decides whether this worker should shutdown or not. 1) it detects whether the server is
   * still alive. If the server got killed because of any reason, the workers will be terminated. 2) it detects whether
   * a shutdown message is received.
   * */
  protected class WorkerLivenessController extends TimerTask {
    private final Timer timer = new Timer();
    private volatile boolean inRun = false;

    @Override
    public void run() {
      if (Worker.this.toShutdown) {
        Worker.this.shutdown();
        ParallelUtility.shutdownVM();
      }
      if (inRun)
        return;
      inRun = true;
      IoSession serverSession = null;
      // int numTry = 0;
      try {
        serverSession = Worker.this.connectionPool.get(0, null, 3, null);
        // while ((serverSession = ParallelUtility.createSession(thisWorker.masterAddress, thisWorker.minaHandler,
        // 3000)) == null
        // && numTry < 3)
        // numTry++;
      } catch (RuntimeException e) {
        // e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (serverSession == null) {
        System.out.println("Cannot connect the server: " + Worker.this.conf.getServer()
            + " Maybe the server is down. I'll shutdown now.");
        System.out.println("Bye!");
        this.timer.cancel();
        ParallelUtility.shutdownVM();
      }
      ParallelUtility.closeSession(serverSession);
      inRun = false;
    }

    public void start() {
      try {
        timer.schedule(this, (long) (Math.random() * 3000) + 50000, // initial
                                                                    // delay
            (long) (Math.random() * 2000) + 10000); // subsequent
                                                    // rate
      } catch (IllegalStateException e) {// already get canceled, ignore
      }
    }
  }

  /**
   * Mina acceptor handler. This is where all messages arriving from other workers and from the coordinator will be
   * processed
   * */
  public class WorkerHandler extends IoHandlerAdapter {

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {
      System.out.println("exception caught");
      cause.printStackTrace();
      ParallelUtility.closeSession(session);
    }

    /**
     * Got called everytime a message is received.
     * */
    @Override
    public void messageReceived(IoSession session, Object message) {
      if (message instanceof TransportMessage) {
        TransportMessage tm = (TransportMessage) message;
        if ((tm.getType() == TransportMessage.TransportMessageType.CONTROL)
            && (ControlMessage.ControlMessageType.CONNECT == tm.getControl().getType())) {
          // connect request sent from other workers
          ControlMessage cm = tm.getControl();
          // ControlProto.ExchangePairID epID = cm.getExchangePairID();

          // ExchangePairID operatorID = ExchangePairID.fromExisting(epID.getExchangePairID());
          // String senderID = epID.getWorkerID();
          session.setAttribute("remoteID", cm.getRemoteID());
          // session.setAttribute("operatorID", operatorID);
        } else {
          Integer senderID = (Integer) session.getAttribute("remoteID");
          if (senderID != null) {
            MessageWrapper mw = new MessageWrapper();
            mw.senderID = senderID;
            mw.message = (TransportMessage) message;
            // ExchangePairID operatorID = (ExchangePairID) session.getAttribute("operatorID");
            // mw.operatorPairID = operatorID;
            Worker.this.messageBuffer.add(mw);
          } else {// currently messages from the master do not have id
            // messages from master
            // System.err.println("Error: message received from an unknown unit: " + message);

            MessageWrapper mw = new MessageWrapper();
            mw.senderID = 0;
            mw.message = (TransportMessage) message;
            // ExchangePairID operatorID = (ExchangePairID) session.getAttribute("operatorID");
            // mw.operatorPairID = null;
            Worker.this.messageBuffer.add(mw);
          }
        }
      } else {
        System.err.println("Error: Unknown message received: " + message);
      }

    }
  }

  public static class MessageWrapper {
    protected int senderID;
    // protected ExchangePairID operatorPairID;
    protected TransportMessage message;
  }

  protected void sendMessageToMaster(TransportMessage message, IoFutureListener<?> callback) {
    System.out.println("send back query ready");
    IoSession s = null;
    s = Worker.this.connectionPool.get(0, null, 3, null);
    if (callback != null)
      s.write(message).addListener(callback);
    else
      s.write(message);
  }

  public static void main(String[] args) throws Throwable {
    if (args.length > 2) {
      System.out.println("Invalid number of arguments.\n" + usage);
      ParallelUtility.shutdownVM();
    }

    File confDir = null;
    if (args.length >= 2)
      if (args[0].equals("--conf")) {
        // dataDir = args[3];
        confDir = new File(args[1]);
        args = ParallelUtility.removeArg(args, 0);
        args = ParallelUtility.removeArg(args, 0);
      } else {
        System.out.println("Invalid arguments.\n" + usage);
        ParallelUtility.shutdownVM();
      }

    Configuration conf = new Configuration(confDir);
    // Instantiate a new worker
    Worker w = new Worker(conf);
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

  /**
   * Initiallize
   * */
  public void init() throws IOException {
    acceptor.setHandler(minaHandler);
  }

  public void start() throws IOException {
    acceptor.bind(conf.getWorkers().get(myID).getAddress());
    queryExecutor.start();
    messageProcessor.start();
    // Periodically detect if the server (i.e., coordinator)
    // is still running. IF the server goes down, the
    // worker will stop itself
    // new WorkerLivenessController().start();

  }

  /**
   * The ID of this worker
   * */
  final int myID;
  //
  // final int port;
  // final String host;
  // /**
  // * The server address
  // * */
  // final InetSocketAddress masterAddress;

  /**
   * connectionPool[0] is always the master
   * */
  protected final ConnectionPool connectionPool;

  /**
   * The acceptor, which binds to a TCP socket and waits for connections
   * 
   * The Server sends messages, including query plans and control messages, to the worker through this acceptor.
   * 
   * Other workers send tuples during query execution to this worker also through this acceptor.
   * */
  final NioSocketAcceptor acceptor;

  /**
   * The current query plan
   * */
  private volatile Operator queryPlan = null;

  /**
   * A indicator of shutting down the worker
   * */
  private volatile boolean toShutdown = false;

  /**
   * Return true if the worker is now executing a query.
   * */
  public boolean isRunning() {
    return this.queryPlan != null;
  }

  public final QueryExecutor queryExecutor;
  public final MessageProcessor messageProcessor;

  /**
   * The IoHandler for network communication. The methods of this handler are called when some communication events
   * happen. For example a message is received, a new connection is created, etc.
   * */
  final WorkerHandler minaHandler;

  /**
   * The I/O buffer, all the ExchangeMessages sent to this worker are buffered here.
   * */
  protected final HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>> dataBuffer;
  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;
  protected final LinkedBlockingQueue<MessageWrapper> messageBuffer;

  protected final Configuration conf;

  public Worker(Configuration conf) {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);
    this.conf = conf;
    this.myID = Integer.parseInt(conf.get("worker.identifier"));

    // port = workerArray[this.workerID - 1].getPort();
    // host = workerArray[this.workerID - 1].getHost();
    // String[] server = conf.get("master.identifier").split(":");
    this.dataDir = new File(conf.get("worker.data.sqlite.dir"));
    this.tmpDir = new File(conf.get("worker.tmp.dir"));
    // this.masterAddress = new InetSocketAddress(server[0], Integer.parseInt(server[1]));
    // this.masterAddress = conf.getServer();

    acceptor = ParallelUtility.createAcceptor();

    dataBuffer = new HashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>>();
    messageBuffer = new LinkedBlockingQueue<MessageWrapper>();
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    queryExecutor = new QueryExecutor();
    queryExecutor.setDaemon(true);
    messageProcessor = new MessageProcessor();
    messageProcessor.setDaemon(false);
    minaHandler = new WorkerHandler();

    Map<Integer, SocketInfo> workers = conf.getWorkers();
    Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, conf.getServer());

    Map<Integer, IoHandler> handlers = new HashMap<Integer, IoHandler>(workers.size() + 1);
    for (Integer workerID : workers.keySet()) {
      handlers.put(workerID, minaHandler);
    }

    handlers.put(0, minaHandler);

    this.connectionPool = new ConnectionPool(myID, computingUnits, handlers);
  }

  /**
   * localize the received query plan. Some information that are required to get the query plan executed need to be
   * replaced by local versions. For example, the table in the SeqScan operator need to be replaced by the local table.
   * Note that Producers and Consumers also needs local information.
   * */
  public void localizeQueryPlan(Operator queryPlan) {
    if (queryPlan == null)
      return;

    if (queryPlan instanceof SQLiteQueryScan) {
      SQLiteQueryScan ss = ((SQLiteQueryScan) queryPlan);
      ss.setDataDir(dataDir.getAbsolutePath());
    } else if (queryPlan instanceof SQLiteSQLProcessor) {
      SQLiteSQLProcessor ss = ((SQLiteSQLProcessor) queryPlan);
      ss.setDataDir(dataDir.getAbsolutePath());
    } else if (queryPlan instanceof Producer) {
      ((Producer) queryPlan).setThisWorker(Worker.this);
    } else if (queryPlan instanceof Consumer) {
      Consumer c = (Consumer) queryPlan;

      LinkedBlockingQueue<ExchangeTupleBatch> buf = null;
      // synchronized (Worker.this) {
      buf = Worker.this.dataBuffer.get(((Consumer) queryPlan).getOperatorID());
      // }
      c.setInputBuffer(buf);
      this.exchangeSchema.put(c.getOperatorID(), c.getSchema());

    } else if (queryPlan instanceof BlockingDataReceiver) {
      BlockingDataReceiver bdr = (BlockingDataReceiver) queryPlan;
      _TupleBatch outputBuffer = bdr.getOutputBuffer();
      if (outputBuffer instanceof SQLiteTupleBatch) {
        ((SQLiteTupleBatch) outputBuffer).reset(dataDir.getAbsolutePath());
      }
    }

    Operator[] children = null;
    if (queryPlan instanceof Operator)
      children = queryPlan.getChildren();

    if (children != null)
      for (Operator child : children)
        if (child != null)
          localizeQueryPlan(child);
  }

  /**
   * Find out all the ParallelOperatorIDs of all consuming operators: ShuffleConsumer, CollectConsumer, and
   * BloomFilterConsumer running at this worker. The inBuffer needs the IDs to distribute the ExchangeMessages received.
   * */
  public static void collectConsumerOperatorIDs(Operator root, ArrayList<ExchangePairID> oIds) {
    if (root instanceof Consumer)
      oIds.add(((Consumer) root).getOperatorID());
    if (root instanceof Operator) {
      Operator[] ops = root.getChildren();
      if (ops != null)
        for (Operator c : ops) {
          if (c != null)
            collectConsumerOperatorIDs(c, oIds);
          else
            return;
        }
    }
  }

  /**
   * execute the current query, note that this method is invoked by the Mina IOHandler thread. Typically, IOHandlers
   * should focus on accepting/routing IO requests, rather than do heavily loaded work.
   * */
  public void executeQuery() {
    System.out.println("Query started");
    synchronized (Worker.this.queryExecutor) {
      Worker.this.queryExecutor.notifyAll();
    }
  }

  /**
   * This method should be called whenever the system is going to shutdown
   * */
  public void shutdown() {
    System.out.println("Shutdown requested. Please wait when cleaning up...");
    ParallelUtility.unbind(acceptor);
    this.toShutdown = true;
  }

  /**
   * this method should be called when a query is received from the server.
   * 
   * It does the initialization and preparation for the execution of the query.
   * */
  public void receiveQuery(Operator query) {
    System.out.println("Query received");
    if (Worker.this.queryPlan != null) {
      System.err.println("Error: Worker is still processing. New query refused");
      return;
    }

    ArrayList<ExchangePairID> ids = new ArrayList<ExchangePairID>();
    collectConsumerOperatorIDs(query, ids);
    Worker.this.dataBuffer.clear();
    Worker.this.exchangeSchema.clear();
    for (ExchangePairID id : ids) {
      Worker.this.dataBuffer.put(id, new LinkedBlockingQueue<ExchangeTupleBatch>());
    }
    Worker.this.localizeQueryPlan(query);
    Worker.this.queryPlan = query;
  }

  /**
   * This method should be called when a data item is received
   * */
  public void receiveData(ExchangeMessage data) {
    if (data instanceof _TupleBatch)
      System.out.println("TupleBag received from " + data.getWorkerID() + " to Operator: " + data.getOperatorID());
    // else if (data instanceof BloomFilterBitSet)
    // System.out.println("BitSet received from " + data.getWorkerID() + " to Operator: "
    // + data.getOperatorID());
    LinkedBlockingQueue<ExchangeTupleBatch> q = null;
    q = Worker.this.dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeTupleBatch)
      q.offer((ExchangeTupleBatch) data);
  }

  /**
   * This method should be called when a query is finished
   * */
  public void finishQuery() {
    if (this.queryPlan != null) {
      this.dataBuffer.clear();
      this.queryPlan = null;
    }
    System.out.println("My part of the query finished");
  }

}
