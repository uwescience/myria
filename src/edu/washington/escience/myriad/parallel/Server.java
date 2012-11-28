package edu.washington.escience.myriad.parallel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.coordinator.catalog.Catalog;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.ControlProto;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.QueryProto;
import edu.washington.escience.myriad.proto.TransportProto;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The parallel system is consisted of one server and a bunch of workers.
 * 
 * The server accepts SQL from commandline, generate a query plan, optimize it, parallelize it. And then the server send
 * a query plan to each worker. Each worker may receive different query plans. The server does not execute any part of a
 * query. The server then receives query results from the workers and display them in the commandline.
 * 
 * The data of a database is averagely spread over all the workers. The execution of each query is conducted
 * simultaneously at each worker.
 * 
 * Currently, we only support equi-join.
 * 
 * The server and the workers run in separate processes. They may or may not cross the boundary of computers.
 * 
 * The server address is configurable at conf/server.conf, the default is
 * 
 * localhost:24444
 * 
 * The worker addresses are configurable at conf/workers.conf, one line for each worker. the default are
 * 
 * localhost:24448 localhost:24449
 * 
 * You may add more workers by adding more host:port lines.
 * 
 * You may change the configurations to cross computers. But make that, the server is able to access the workers and
 * each worker is able to access both the server and all other workers.
 * 
 * 
 * Given a query, there can be up to two different query plans. If the query contains order by or aggregates without
 * group by, a "master worker" is needed to collect the tuples to be ordered or to be aggregated from all the workers.
 * All the other workers are correspondingly called slave workers.
 * 
 * For example, for a query:
 * 
 * select * from actor order by id;
 * 
 * at slave workers, the query plan is:
 * 
 * scan actor -> send data to the master worker
 * 
 * at the master worker, the query plan is:
 * 
 * scan actor -> send data to myself -> collect data from all the workers -> order by -> send output to the server
 * 
 * 
 * at slave workers: | at the master worker: | at the server: | | worker_1 : scan actor | | worker_2 : scan actor | |
 * ... | collect -> order by (id) ------| ---> output result worker_(n-1): scan actor | | worker_n : scan actor | |
 * 
 * If there is no order by and aggregate without group by, all the workers will have the same query plan. The results of
 * each worker are sent directly to the server.
 * 
 * 
 * We use Apache Mina for inter-process communication. Mina is a wrapper library of java nio. To start using Mina,
 * http://mina.apache.org/user-guide.html is a good place to seek information.
 * 
 * 
 * 
 */
public final class Server {

  protected final class MessageProcessor extends Thread {
    @Override
    public void run() {

      TERMINATE_MESSAGE_PROCESSING : while (true) {
        MessageWrapper mw = null;
        try {
          mw = messageBuffer.take();
        } catch (final InterruptedException e) {
          e.printStackTrace();
          break TERMINATE_MESSAGE_PROCESSING;
        }
        final TransportMessage m = mw.message;
        final int senderID = mw.senderID;

        switch (m.getType().getNumber()) {
          case TransportMessage.TransportMessageType.DATA_VALUE:

            final DataMessage data = m.getData();
            final ExchangePairID exchangePairID = ExchangePairID.fromExisting(data.getOperatorID());
            final Schema operatorSchema = exchangeSchema.get(exchangePairID);
            if (data.getType() == DataMessageType.EOS) {
              receiveData(new ExchangeTupleBatch(exchangePairID, senderID, operatorSchema));
            } else {
              final List<ColumnMessage> columnMessages = data.getColumnsList();
              final Column<?>[] columnArray = new Column[columnMessages.size()];
              int idx = 0;
              for (final ColumnMessage cm : columnMessages) {
                columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
              }
              final List<Column<?>> columns = Arrays.asList(columnArray);

              receiveData((new ExchangeTupleBatch(exchangePairID, senderID, columns, operatorSchema, columnMessages
                  .get(0).getNumTuples())));
            }
            break;
          case TransportMessage.TransportMessageType.CONTROL_VALUE:
            final ControlMessage controlM = m.getControl();
            switch (controlM.getType().getNumber()) {
              case ControlMessage.ControlMessageType.QUERY_READY_TO_EXECUTE_VALUE:
                queryReceivedByWorker(0, senderID);
                break;
            }
            break;
        }

      }

    }
  }

  protected final class ServerHandler extends IoHandlerAdapter {

    // required in ParallelTest for instrument
    final Thread mainThread;

    public ServerHandler(final Thread mainThread) {
      this.mainThread = mainThread;
    }

    @Override
    public void exceptionCaught(final IoSession session, final Throwable cause) {
      cause.printStackTrace();
      // ParallelUtility.closeSession(session);
    }

    @Override
    public void messageReceived(final IoSession session, final Object message) {

      if (message instanceof TransportMessage) {

        final TransportMessage tm = (TransportMessage) message;
        if ((tm.getType() == TransportMessage.TransportMessageType.CONTROL)
            && (ControlMessage.ControlMessageType.CONNECT == tm.getControl().getType())) {
          // connect request sent from other workers
          final ControlMessage cm = tm.getControl();
          // ControlProto.ExchangePairID epID = cm.getExchangePairID();

          // ExchangePairID operatorID = ExchangePairID.fromExisting(epID.getExchangePairID());
          // String senderID = epID.getWorkerID();
          session.setAttribute("remoteID", cm.getRemoteID());
          // session.setAttribute("operatorID", operatorID);
        } else {
          final Integer senderID = (Integer) session.getAttribute("remoteID");
          if (senderID != null) {
            final MessageWrapper mw = new MessageWrapper();
            mw.senderID = senderID;
            mw.message = tm;
            // ExchangePairID operatorID = (ExchangePairID) session.getAttribute("operatorID");

            messageBuffer.add(mw);
          } else {
            LOGGER.error("Error: message received from an unknown unit: " + message);
          }
        }
      } else {
        LOGGER.error("Error: Unknown message received: " + message);
      }
    }

    @Override
    public void sessionIdle(final IoSession session, final IdleStatus status) {
      if (status.equals(IdleStatus.BOTH_IDLE)) {
        session.close(false);
      }
    }
  }

  static final String usage = "Usage: Server catalogFile [-explain] [-f queryFile]";
  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  public static void main(String[] args) throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    if (args.length < 1) {
      LOGGER.error(usage);
      System.exit(-1);
    }

    String catalogName = args[0];
    args = ParallelUtility.removeArg(args, 0);

    Catalog catalog;
    List<SocketInfo> masters;
    Map<Integer, SocketInfo> workers;
    try {
      catalog = Catalog.open(catalogName);
      masters = catalog.getMasters();
      workers = catalog.getWorkers();
    } catch (CatalogException e) {
      throw new RuntimeException("Reading information from the catalog failed", e);
    }

    if (masters.size() != 1) {
      throw new RuntimeException("Unexpected number of masters: expected 1, got " + masters.size());
    }

    final SocketInfo masterSocketInfo = masters.get(0);
    final Server server = new Server(masterSocketInfo, workers);

    runningInstance = server;

    LOGGER.debug("Workers are: ");
    for (final Entry<Integer, SocketInfo> w : server.workers.entrySet()) {
      LOGGER.debug(w.getKey() + ":  " + w.getValue().getHost() + ":" + w.getValue().getPort());
    }

    server.init();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        server.cleanup();
      }
    });
    server.start(args);
    LOGGER.debug("Server: " + masterSocketInfo.getHost() + " started. Listening on port " + masterSocketInfo.getPort());
  }

  private final ConcurrentHashMap<Integer, SocketInfo> workers;
  private final NioSocketAcceptor acceptor;
  private final ServerHandler minaHandler;
  private final ConcurrentHashMap<Integer, HashMap<Integer, Integer>> workersAssignedToQuery;
  private final ConcurrentHashMap<Integer, BitSet> workersReceivedQuery;

  /**
   * The I/O buffer, all the ExchangeMessages sent to the server are buffered here.
   */
  protected final ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>> dataBuffer;

  protected final LinkedBlockingQueue<MessageWrapper> messageBuffer;

  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  private final SocketInfo server;

  protected final IPCConnectionPool connectionPool;

  protected final MessageProcessor messageProcessor;

  public static volatile Server runningInstance = null;

  protected boolean interactive = true;

  public static final String SYSTEM_NAME = "Myriad";

  protected Server(final SocketInfo server, final Map<Integer, SocketInfo> workers) throws IOException {
    this.workers = new ConcurrentHashMap<Integer, SocketInfo>();
    this.workers.putAll(workers);

    acceptor = ParallelUtility.createAcceptor();
    this.server = server;
    dataBuffer = new ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeTupleBatch>>();
    messageBuffer = new LinkedBlockingQueue<MessageWrapper>();
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    minaHandler = new ServerHandler(Thread.currentThread());

    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, server);

    final Map<Integer, IoHandler> handlers = new HashMap<Integer, IoHandler>(workers.size() + 1);
    for (final Integer workerID : workers.keySet()) {
      handlers.put(workerID, minaHandler);
    }

    handlers.put(0, minaHandler);

    connectionPool = new IPCConnectionPool(0, computingUnits, handlers);
    messageProcessor = new MessageProcessor();
    workersAssignedToQuery = new ConcurrentHashMap<Integer, HashMap<Integer, Integer>>();
    workersReceivedQuery = new ConcurrentHashMap<Integer, BitSet>();
  }

  public void cleanup() {
    LOGGER.debug(SYSTEM_NAME + " is going to shutdown");
    LOGGER.debug("Send shutdown requests to the workers, please wait");
    for (final Entry<Integer, SocketInfo> worker : workers.entrySet()) {
      LOGGER.debug("Shuting down #" + worker.getKey() + " : " + worker.getValue());

      IoSession session = null;
      try {
        session = Server.this.connectionPool.get(worker.getKey(), null, 3, null);
      } catch (final Throwable e) {
      }
      if (session == null) {
        LOGGER.error("Fail to connect the worker: " + worker + ". Continue cleaning");
        continue;
      }
      session.write(
          TransportProto.TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
              ControlProto.ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.SHUTDOWN).build())
              .build()).addListener(new IoFutureListener<WriteFuture>() {
        @Override
        public void operationComplete(final WriteFuture future) {
          ParallelUtility.closeSession(future.getSession());
        }
      });
      LOGGER.debug("Done");
    }
    ParallelUtility.unbind(acceptor);
    LOGGER.debug("Bye");
  }

  public void dispatchWorkerQueryPlans(final Map<Integer, Operator> plans) throws IOException {
    ByteArrayOutputStream inMemBuffer = null;
    ObjectOutputStream oos = null;
    HashMap<Integer, Integer> setOfWorkers = new HashMap<Integer, Integer>(plans.size());
    workersAssignedToQuery.put(0, setOfWorkers);
    workersReceivedQuery.put(0, new BitSet(setOfWorkers.size()));

    int workerIdx = 0;
    for (final Map.Entry<Integer, Operator> e : plans.entrySet()) {
      final Integer workerID = e.getKey();
      setOfWorkers.put(workerID, workerIdx++);
      final Operator plan = e.getValue();
      final IoSession ssss0 = connectionPool.get(workerID, null, 3, null);
      // this session will be reused for the Workers to report the receive
      // of the queryplan, therefore, do not close it
      inMemBuffer = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(inMemBuffer);
      oos.writeObject(plan);
      oos.flush();
      inMemBuffer.flush();
      ssss0.write(TransportMessage.newBuilder().setType(TransportMessageType.QUERY).setQuery(
          QueryProto.Query.newBuilder().setQuery(ByteString.copyFrom(inMemBuffer.toByteArray()))).build());
      // oos.reset();
    }
  }

  protected void init() throws IOException {
    acceptor.setHandler(minaHandler);
  }

  // TODO implement queryID
  protected void queryReceivedByWorker(final int queryId, final int workerId) {
    BitSet workersReceived = workersReceivedQuery.get(queryId);
    HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    int workerIdx = workersAssigned.get(workerId);
    workersReceived.set(workerIdx);
  }

  protected void startWorkerQuery(final int queryId) {
    HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    for (final Entry<Integer, Integer> entry : workersAssigned.entrySet()) {
      Server.this.connectionPool.get(entry.getKey(), null, 3, null).write(
          TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
              ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.START_QUERY).build()).build());
    }
    workersAssignedToQuery.remove(queryId);
    workersReceivedQuery.remove(queryId);
  }

  /**
   * This method should be called when a data item is received
   */
  public void receiveData(final ExchangeTupleBatch data) {

    LinkedBlockingQueue<ExchangeTupleBatch> q = null;
    q = Server.this.dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeTupleBatch) {
      q.offer(data);
    }
  }

  public void shutdown() {
    ParallelUtility.shutdownVM();
  }

  protected void start(final String[] argv) throws IOException {
    acceptor.bind(server.getAddress());
    messageProcessor.start();
  }

  /**
   * @return if the query is successfully executed.
   * */
  public TupleBatchBuffer startServerQuery(int queryId, final CollectConsumer serverPlan) throws DbException {
    BitSet workersReceived = workersReceivedQuery.get(queryId);
    HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    if (workersReceived.nextClearBit(0) >= workersAssigned.size()) {

      final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
      exchangeSchema.put(serverPlan.getOperatorID(), serverPlan.getSchema());
      dataBuffer.put(serverPlan.getOperatorID(), buffer);
      serverPlan.setInputBuffer(buffer);

      final Schema schema = serverPlan.getSchema();

      String names = "";
      for (int i = 0; i < schema.numFields(); i++) {
        names += schema.getFieldName(i) + "\t";
      }

      if (LOGGER.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append(names).append('\n');
        for (int i = 0; i < names.length() + schema.numFields() * 4; i++) {
          sb.append("-");
        }
        sb.append("");
        LOGGER.debug(sb.toString());
      }

      Date start = new Date();
      serverPlan.open();

      startWorkerQuery(queryId);

      TupleBatchBuffer outBufferForTesting = new TupleBatchBuffer(serverPlan.getSchema());
      int cnt = 0;
      _TupleBatch tup = null;
      while ((tup = serverPlan.next()) != null) {
        outBufferForTesting.putAll(tup);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(new ImmutableInMemoryTupleBatch(serverPlan.getSchema(), tup.outputRawData(), tup
              .numOutputTuples()).toString());
        }
        cnt += tup.numOutputTuples();
      }

      serverPlan.close();
      Server.this.dataBuffer.remove(serverPlan.getOperatorID());
      Date end = new Date();
      LOGGER.debug("Number of results: " + cnt);
      System.out.println("Number of results: " + cnt);
      int elapse = (int) (end.getTime() - start.getTime());
      int hour = elapse / 3600000;
      elapse -= hour * 3600000;
      int minute = elapse / 60000;
      elapse -= minute * 60000;
      int second = elapse / 1000;
      elapse -= second * 1000;

      LOGGER.debug(String.format("Time elapsed: %1$dh%2$dm%3$ds.%4$03d", hour, minute, second, elapse));
      System.out.println(String.format("Time elapsed: %1$dh%2$dm%3$ds.%4$03d", hour, minute, second, elapse));
      return outBufferForTesting;
    } else {
      return null;
    }
  }
}