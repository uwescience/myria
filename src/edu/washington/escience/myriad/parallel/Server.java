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

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
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
import edu.washington.escience.myriad.util.JVMUtils;

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
          mw = messageQueue.take();
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
              receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema));
            } else {
              final List<ColumnMessage> columnMessages = data.getColumnsList();
              final Column<?>[] columnArray = new Column[columnMessages.size()];
              int idx = 0;
              for (final ColumnMessage cm : columnMessages) {
                columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm);
              }
              final List<Column<?>> columns = Arrays.asList(columnArray);

              receiveData((new ExchangeData(exchangePairID, senderID, columns, operatorSchema, columnMessages.get(0)
                  .getNumTuples())));
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

  final ConcurrentHashMap<Integer, SocketInfo> workers;
  final ServerBootstrap ipcServer;
  private Channel ipcServerChannel;
  final ConcurrentHashMap<Integer, HashMap<Integer, Integer>> workersAssignedToQuery;
  final ConcurrentHashMap<Integer, BitSet> workersReceivedQuery;

  /**
   * The I/O buffer, all the ExchangeMessages sent to the server are buffered here.
   */
  protected final ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>> dataBuffer;

  protected final LinkedBlockingQueue<MessageWrapper> messageQueue;

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

    this.server = server;
    dataBuffer = new ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>>();
    messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    ipcServer = ParallelUtility.createMasterIPCServer(messageQueue);
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, server);

    connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    messageProcessor = new MessageProcessor();
    workersAssignedToQuery = new ConcurrentHashMap<Integer, HashMap<Integer, Integer>>();
    workersReceivedQuery = new ConcurrentHashMap<Integer, BitSet>();
  }

  public void cleanup() {
    LOGGER.debug(SYSTEM_NAME + " is going to shutdown");
    LOGGER.debug("Send shutdown requests to the workers, please wait");
    for (final Entry<Integer, SocketInfo> worker : workers.entrySet()) {
      LOGGER.debug("Shuting down #" + worker.getKey() + " : " + worker.getValue());

      Channel ch = null;
      try {
        ch = Server.this.connectionPool.get(worker.getKey(), 3, null);
      } catch (final Throwable e) {
      }
      if (ch == null || !ch.isConnected()) {
        LOGGER.error("Fail to connect the worker: " + worker + ". Continue cleaning");
        continue;
      }
      ch.write(
          TransportProto.TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
              ControlProto.ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.SHUTDOWN).build())
              .build()).awaitUninterruptibly();
      LOGGER.debug("Done");
    }
    ParallelUtility.shutdownIPC(ipcServerChannel, connectionPool);
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
      final Channel ssss0 = connectionPool.get(workerID, 3, null);
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
      Server.this.connectionPool.get(entry.getKey(), 3, null).write(
          TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
              ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.START_QUERY).build()).build());
    }
    workersAssignedToQuery.remove(queryId);
    workersReceivedQuery.remove(queryId);
  }

  /**
   * This method should be called when a data item is received
   */
  public void receiveData(final ExchangeData data) {

    LinkedBlockingQueue<ExchangeData> q = null;
    q = Server.this.dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeData) {
      q.offer(data);
    }
  }

  public void shutdown() {
    JVMUtils.shutdownVM();
  }

  protected void start(final String[] argv) throws IOException {
    ipcServerChannel = ipcServer.bind(server.getAddress());
    messageProcessor.start();
  }

  /**
   * @return if the query is successfully executed.
   * */
  public TupleBatchBuffer startServerQuery(int queryId, final CollectConsumer serverPlan) throws DbException {
    BitSet workersReceived = workersReceivedQuery.get(queryId);
    HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    if (workersReceived.nextClearBit(0) >= workersAssigned.size()) {

      final LinkedBlockingQueue<ExchangeData> buffer = new LinkedBlockingQueue<ExchangeData>();
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
      TupleBatch tup = null;
      while ((tup = serverPlan.next()) != null) {
        tup.compactInto(outBufferForTesting);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(tup.toString());
        }
        cnt += tup.numTuples();
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