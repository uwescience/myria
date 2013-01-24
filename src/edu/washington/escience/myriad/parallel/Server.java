package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.LoggerFactory;

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
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

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

  private final class MessageProcessor extends Thread {
    /** Whether the server has been stopped. */
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
        final int senderID = mw.senderID;

        switch (m.getType()) {
          case DATA:

            final DataMessage data = m.getData();
            final ExchangePairID exchangePairID = ExchangePairID.fromExisting(data.getOperatorID());
            final Schema operatorSchema = exchangeSchema.get(exchangePairID);
            if (data.getType() == DataMessageType.EOS) {
              receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, 0));
            } else if (data.getType() == DataMessageType.EOI) {
              receiveData(new ExchangeData(exchangePairID, senderID, operatorSchema, 1));
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
          case CONTROL:
            final ControlMessage controlM = m.getControl();
            final Long queryId = controlM.getQueryId();
            switch (controlM.getType()) {
              case QUERY_READY_TO_EXECUTE:
                queryReceivedByWorker(queryId, senderID);
                break;
              case WORKER_ALIVE:
                aliveWorkers.add(senderID);
                break;
              case QUERY_COMPLETE:
                HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(controlM.getQueryId());
                if (!workersAssigned.containsKey(senderID)) {
                  ;// TODO complain about something bad.
                }
                workersAssigned.remove(senderID);
                break;
              case DISCONNECT:
                /* TODO */
                break;
              case CONNECT:
              case SHUTDOWN:
              case START_QUERY:
                throw new RuntimeException("Unexpected control message received at server: " + controlM.toString());
            }
            break;
          case QUERY:
            throw new RuntimeException("Unexpected query message received at server");
        }

      }

    }

    public void setStopped() {
      stopped = true;
    }
  }

  /** Time constant. */
  private static final int ONE_SEC_IN_MILLIS = 1000;
  /** Time constant. */
  private static final int ONE_MIN_IN_MILLIS = 60 * ONE_SEC_IN_MILLIS;
  /** Time constant. */
  private static final int ONE_HR_IN_MILLIS = 60 * ONE_MIN_IN_MILLIS;

  static final String usage = "Usage: Server catalogFile [-explain] [-f queryFile]";
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Server.class.getName());

  public static void main(final String[] args) throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    if (args.length < 1) {
      LOGGER.error(usage);
      System.exit(-1);
    }

    final String catalogName = args[0];

    final Server server = new Server(catalogName);

    LOGGER.debug("Workers are: ");
    for (final Entry<Integer, SocketInfo> w : server.workers.entrySet()) {
      LOGGER.debug(w.getKey() + ":  " + w.getValue().getHost() + ":" + w.getValue().getPort());
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        server.cleanup();
      }
    });
    server.start();
    LOGGER.debug("Server started.");
  }

  final ConcurrentHashMap<Integer, SocketInfo> workers;
  final ConcurrentHashMap<Long, HashMap<Integer, Integer>> workersAssignedToQuery;
  final Set<Integer> aliveWorkers;
  final ConcurrentHashMap<Long, BitSet> workersReceivedQuery;

  /**
   * The I/O buffer, all the ExchangeMessages sent to the server are buffered here.
   */
  protected final ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>> dataBuffer;

  protected final LinkedBlockingQueue<MessageWrapper> messageQueue;

  protected final ConcurrentHashMap<ExchangePairID, Schema> exchangeSchema;

  private final IPCConnectionPool connectionPool;

  protected final MessageProcessor messageProcessor;

  protected boolean interactive = true;

  public static final String SYSTEM_NAME = "Myriad";

  private volatile boolean cleanup = false;

  public Server(final String catalogFileName) throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    Catalog catalog;
    List<SocketInfo> masters;
    Map<Integer, SocketInfo> workers;
    try {
      catalog = Catalog.open(catalogFileName);
      masters = catalog.getMasters();
      workers = catalog.getWorkers();
      catalog.close();
    } catch (final CatalogException e) {
      throw new RuntimeException("Reading information from the catalog failed", e);
    }

    if (masters.size() != 1) {
      throw new RuntimeException("Unexpected number of masters: expected 1, got " + masters.size());
    }

    final SocketInfo hostPort = masters.get(0);
    this.workers = new ConcurrentHashMap<Integer, SocketInfo>(workers);
    aliveWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    dataBuffer = new ConcurrentHashMap<ExchangePairID, LinkedBlockingQueue<ExchangeData>>();
    messageQueue = new LinkedBlockingQueue<MessageWrapper>();
    exchangeSchema = new ConcurrentHashMap<ExchangePairID, Schema>();

    final Map<Integer, SocketInfo> computingUnits = new HashMap<Integer, SocketInfo>();
    computingUnits.putAll(workers);
    computingUnits.put(0, hostPort);

    connectionPool = new IPCConnectionPool(0, computingUnits, messageQueue);
    messageProcessor = new MessageProcessor();
    workersAssignedToQuery = new ConcurrentHashMap<Long, HashMap<Integer, Integer>>();
    workersReceivedQuery = new ConcurrentHashMap<Long, BitSet>();
  }

  public void cleanup() {
    if (cleanup) {
      return;
    }
    cleanup = true;
    LOGGER.debug(SYSTEM_NAME + " is going to shutdown");
    LOGGER.debug("Send shutdown requests to the workers, please wait");
    for (int workerId : aliveWorkers) {
      LOGGER.debug("Shutting down #" + workerId);

      final ChannelFuture cf = getConnectionPool().sendShortMessage(workerId, IPCUtils.CONTROL_SHUTDOWN);
      if (cf == null) {
        LOGGER.error("Fail to connect the worker: " + workerId + ". Continue cleaning");
      } else {
        cf.awaitUninterruptibly();
        LOGGER.debug("Done for worker #" + workerId);
      }
    }
    connectionPool.shutdown().awaitUninterruptibly();
    LOGGER.debug("Bye");
  }

  public void dispatchWorkerQueryPlans(final Long queryId, final Map<Integer, Operator[]> plans) throws IOException {
    final HashMap<Integer, Integer> setOfWorkers = new HashMap<Integer, Integer>(plans.size());
    workersAssignedToQuery.put(queryId, setOfWorkers);
    workersReceivedQuery.put(queryId, new BitSet(setOfWorkers.size()));

    int workerIdx = 0;
    for (final Map.Entry<Integer, Operator[]> e : plans.entrySet()) {
      final Integer workerID = e.getKey();
      setOfWorkers.put(workerID, workerIdx++);
      while (!aliveWorkers.contains(workerID)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
      getConnectionPool().sendShortMessage(workerID, IPCUtils.queryMessage(queryId, e.getValue()));
    }
  }

  /**
   * @return the network Connection Pool used by the Server.
   */
  public IPCConnectionPool getConnectionPool() {
    return connectionPool;
  }

  // TODO implement queryID
  protected void queryReceivedByWorker(final Long queryId, final int workerId) {
    final BitSet workersReceived = workersReceivedQuery.get(queryId);
    final HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    final int workerIdx = workersAssigned.get(workerId);
    workersReceived.set(workerIdx);
  }

  /**
   * This method should be called when a data item is received
   */
  public void receiveData(final ExchangeData data) {

    LinkedBlockingQueue<ExchangeData> q = null;
    q = dataBuffer.get(data.getOperatorID());
    if (data instanceof ExchangeData) {
      q.offer(data);
    }
  }

  public boolean queryCompleted(final Long queryId) {
    if (workersAssignedToQuery.containsKey(queryId)) {
      return workersAssignedToQuery.get(queryId).isEmpty();
    }
    return true;
  }

  public void shutdown() {
    messageProcessor.setStopped();
    cleanup();
  }

  public void start() throws IOException {
    // ipcServerChannel = ipcServer.bind(server.getAddress());
    connectionPool.start();
    messageProcessor.start();
  }

  /**
   * @return if the query is successfully executed.
   * */
  public TupleBatchBuffer startServerQuery(final Long queryId, final CollectConsumer serverPlan) throws DbException {
    final BitSet workersReceived = workersReceivedQuery.get(queryId);
    final HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    for (Integer workerId : workersAssigned.keySet()) {
      if (!aliveWorkers.contains(workerId)) {
        return null;
      }
    }

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
        final StringBuilder sb = new StringBuilder();
        sb.append(names).append('\n');
        for (int i = 0; i < names.length() + schema.numFields() * 4; i++) {
          sb.append("-");
        }
        sb.append("");
        LOGGER.debug(sb.toString());
      }

      final Date start = new Date();
      serverPlan.open();

      startWorkerQuery(queryId);

      final TupleBatchBuffer outBufferForTesting = new TupleBatchBuffer(serverPlan.getSchema());
      int cnt = 0;
      TupleBatch tup = null;
      while (!serverPlan.eos()) {
        while ((tup = serverPlan.next()) != null) {
          tup.compactInto(outBufferForTesting);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(tup.toString());
          }
          cnt += tup.numTuples();
        }
        if (serverPlan.eoi()) {
          serverPlan.setEOI(false);
        }
      }

      serverPlan.close();
      dataBuffer.remove(serverPlan.getOperatorID());
      final Date end = new Date();
      LOGGER.info("Number of results: " + cnt);
      int elapse = (int) (end.getTime() - start.getTime());
      final int hour = elapse / ONE_HR_IN_MILLIS;
      elapse -= hour * ONE_HR_IN_MILLIS;
      final int minute = elapse / ONE_MIN_IN_MILLIS;
      elapse -= minute * ONE_MIN_IN_MILLIS;
      final int second = elapse / ONE_SEC_IN_MILLIS;
      elapse -= second * ONE_SEC_IN_MILLIS;

      LOGGER.info(String.format("Time elapsed: %1$dh%2$dm%3$ds.%4$03d", hour, minute, second, elapse));
      return outBufferForTesting;
    } else {
      return null;
    }
  }

  /**
   * This starts a query from the server using the query plan rooted by the given Producer.
   * 
   * @param queryId the id of this query. TODO currently always 0.
   * @param serverPlan the query plan to be executed.
   * @return true if the query is successfully executed.
   * @throws DbException if there are errors executing the serverPlan operators.
   */
  public boolean startServerQuery(final Long queryId, final Producer serverPlan) throws DbException {
    final BitSet workersReceived = workersReceivedQuery.get(queryId);
    final HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    for (Integer workerId : workersAssigned.keySet()) {
      if (!aliveWorkers.contains(workerId)) {
        return false;
      }
    }
    if (workersReceived.nextClearBit(0) >= workersAssigned.size()) {

      final Date start = new Date();

      serverPlan.open();

      startWorkerQuery(queryId);

      while (serverPlan.next() != null) {
        /* Do nothing. */
      }

      serverPlan.close();

      final Date end = new Date();
      int elapse = (int) (end.getTime() - start.getTime());
      final int hour = elapse / ONE_HR_IN_MILLIS;
      elapse -= hour * ONE_HR_IN_MILLIS;
      final int minute = elapse / ONE_MIN_IN_MILLIS;
      elapse -= minute * ONE_MIN_IN_MILLIS;
      final int second = elapse / ONE_SEC_IN_MILLIS;
      elapse -= second * ONE_SEC_IN_MILLIS;

      LOGGER.debug(String.format("Time elapsed: %1$dh%2$dm%3$ds.%4$03d", hour, minute, second, elapse));
      return true;
    } else {
      return false;
    }
  }

  protected void startWorkerQuery(final Long queryId) {
    final HashMap<Integer, Integer> workersAssigned = workersAssignedToQuery.get(queryId);
    for (final Entry<Integer, Integer> entry : workersAssigned.entrySet()) {
      getConnectionPool().sendShortMessage(entry.getKey(), IPCUtils.startQueryTM(0, queryId));
    }
  }
}