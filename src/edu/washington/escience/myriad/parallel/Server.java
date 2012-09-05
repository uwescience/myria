package edu.washington.escience.myriad.parallel;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.util.ConcurrentHashSet;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.Exchange.ParallelOperatorID;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The parallel system is consisted of one server and a bunch of workers.
 * 
 * The server accepts SQL from commandline, generate a query plan, optimize it, parallelize it. And
 * then the server send a query plan to each worker. Each worker may receive different query plans.
 * The server does not execute any part of a query. The server then receives query results from the
 * workers and display them in the commandline.
 * 
 * The data of a database is averagely spread over all the workers. The execution of each query is
 * conducted simultaneously at each worker.
 * 
 * Currently, we only support equi-join.
 * 
 * The server and the workers run in separate processes. They may or may not cross the boundary of
 * computers.
 * 
 * The server address is configurable at conf/server.conf, the default is
 * 
 * localhost:24444
 * 
 * The worker addresses are configurable at conf/workers.conf, one line for each worker. the default
 * are
 * 
 * localhost:24448 localhost:24449
 * 
 * You may add more workers by adding more host:port lines.
 * 
 * You may change the configurations to cross computers. But make that, the server is able to access
 * the workers and each worker is able to access both the server and all other workers.
 * 
 * 
 * Given a query, there can be up to two different query plans. If the query contains order by or
 * aggregates without group by, a "master worker" is needed to collect the tuples to be ordered or
 * to be aggregated from all the workers. All the other workers are correspondingly called slave
 * workers.
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
 * scan actor -> send data to myself -> collect data from all the workers -> order by -> send output
 * to the server
 * 
 * 
 * at slave workers: | at the master worker: | at the server: | | worker_1 : scan actor | | worker_2
 * : scan actor | | ... | collect -> order by (id) ------| ---> output result worker_(n-1): scan
 * actor | | worker_n : scan actor | |
 * 
 * If there is no order by and aggregate without group by, all the workers will have the same query
 * plan. The results of each worker are sent directly to the server.
 * 
 * 
 * We use Apache Mina for inter-process communication. Mina is a wrapper library of java nio. To
 * start using Mina, http://mina.apache.org/user-guide.html is a good place to seek information.
 * 
 * 
 * 
 * */
public class Server {

  static final String usage =
      "Usage: Server catalogFile [--conf confdir] [-explain] [-f queryFile]";

  final SocketInfo[] workers;
  final HashMap<String, Integer> workerIdToIndex;
  final NioSocketAcceptor acceptor;
  final ServerHandler minaHandler;

  /**
   * The I/O buffer, all the ExchangeMessages sent to the server are buffered here.
   * */
  final ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<_TupleBatch>> inBuffer;
  final SocketInfo server;

  public static final String DEFAULT_CONF_DIR = "conf";

  public static SocketInfo loadServer(String confDir) throws IOException {
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(confDir
            + "/server.conf"))));
    String line = null;
    while ((line = br.readLine()) != null) {
      String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "").split(":");
      if (ts.length == 2)
        return new SocketInfo(ts[0], Integer.parseInt(ts[1]));
    }
    throw new IOException("Wrong server conf file.");
  }

  protected Server(String host, int port, SocketInfo[] workers) throws IOException {
    this.workers = workers;
    workerIdToIndex = new HashMap<String, Integer>();
    for (int i = 0; i < workers.length; i++)
      workerIdToIndex.put(workers[i].getId(), i);
    acceptor = ParallelUtility.createAcceptor();
    this.server = new SocketInfo(host, port);
    this.inBuffer =
        new ConcurrentHashMap<ParallelOperatorID, LinkedBlockingQueue<_TupleBatch>>();
    this.minaHandler = new ServerHandler(Thread.currentThread());
  }

  protected void init() throws IOException {
    // Bind

    acceptor.setHandler(this.minaHandler);
    acceptor.bind(this.server.getAddress());
  }

  public static void main(String[] args) throws IOException {
//    if (args.length < 1 || args.length > 6) {
//      System.err.println("Invalid number of arguments.\n" + usage);
//      System.exit(0);
//    }

    String confDir = DEFAULT_CONF_DIR;
    if (args.length >= 3 && args[1].equals("--conf")) {
      confDir = args[2];
      args = ParallelUtility.removeArg(args, 1);
      args = ParallelUtility.removeArg(args, 1);
    }

    SocketInfo serverInfo = loadServer(confDir);
    final Server server =
        new Server(serverInfo.getHost(), serverInfo.getPort(), ParallelUtility.loadWorkers(confDir));

    runningInstance=server;
    
    System.out.println("Workers are: ");
    for (SocketInfo w : server.workers)
      System.out.println("  " + w.getHost() + ":" + w.getPort());

    server.init();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        server.cleanup();
      }
    });
    System.out.println("Server: " + server.server.getHost() + " started. Listening on port "
        + serverInfo.getPort());
    server.start(args);
  }
  
  public static volatile Server runningInstance =null;

  protected boolean interactive=true;
  // Basic SQL completions
  public static final String[] SQL_COMMANDS = {
      "select", "from", "where", "group by", "max(", "min(", "avg(", "count", "rollback", "commit",
      "insert", "delete", "values", "into"};
  public static final String SYSTEM_NAME = "Myriad";

  protected void start(String[] argv) throws IOException {
    // first add tables to database
    // Database.getCatalog().loadSchema(argv[0]);
    // TableStats.computeStatistics();

    String queryFile = null;

    if (argv.length > 1) {
      for (int i = 1; i < argv.length; i++) {
        if (argv[i].equals("-f")) {
          interactive = false;
          if (i++ == argv.length) {
            System.out.println("Expected file name after -f\n" + usage);
            System.exit(0);
          }
          queryFile = argv[i];

        } else {
          System.out.println("Unknown argument " + argv[i] + "\n " + usage);
        }
      }
    }

    if (!interactive) {
      try {
        // curtrans = new Transaction();
        // curtrans.start();
        long startTime = System.currentTimeMillis();
        processNextStatement(new FileInputStream(new File(queryFile)));
        long time = System.currentTimeMillis() - startTime;
        System.out.printf("----------------\n%.2f seconds\n\n", ((double) time / 1000.0));
        System.out.println("Press Enter to exit");
        System.in.read();
        this.shutdown();
      } catch (FileNotFoundException e) {
        System.out.println("Unable to find query file" + queryFile);
        e.printStackTrace();
      }
    } else { // no query file, run interactive prompt
      ConsoleReader reader = new ConsoleReader();

      // Add really stupid tab completion for simple SQL
      ArgumentCompletor completor = new ArgumentCompletor(new SimpleCompletor(SQL_COMMANDS));
      completor.setStrict(false); // match at any position
      reader.addCompletor(completor);

      StringBuilder buffer = new StringBuilder();
      String line;
      boolean quit = false;
      while (!quit && (line = reader.readLine(SYSTEM_NAME + "> ")) != null) {
        // Split statements at ';': handles multiple statements on one
        // line, or one
        // statement spread across many lines
        while (line.indexOf(';') >= 0) {
          int split = line.indexOf(';');
          buffer.append(line.substring(0, split + 1));
          String cmd = buffer.toString().trim();
          cmd = cmd.substring(0, cmd.length() - 1).trim() + ";";
          byte[] statementBytes = cmd.getBytes("UTF-8");
          if (cmd.equalsIgnoreCase("quit;") || cmd.equalsIgnoreCase("exit;")) {
            shutdown();
            quit = true;
            break;
          }

          long startTime = System.currentTimeMillis();
          processNextStatement(new ByteArrayInputStream(statementBytes));
          long time = System.currentTimeMillis() - startTime;
          System.out.printf("----------------\n%.2f seconds\n\n", ((double) time / 1000.0));

          // Grab the remainder of the line
          line = line.substring(split + 1);
          buffer = new StringBuilder();
        }
        if (line.length() > 0) {
          buffer.append(line);
          buffer.append("\n");
        }
      }
    }
  }

  public void shutdown() {
    ParallelUtility.shutdownVM();
  }

  public void cleanup() {
    System.out.println(SYSTEM_NAME + " is going to shutdown");
    System.out.println("Send shutdown requests to the workers, please wait");
    for (SocketInfo worker : workers) {
      System.out.println("Shuting down " + worker.getId());

      IoSession session = null;
      try
      {
          session = ParallelUtility.createSession(worker.getAddress(), this.minaHandler, 3000);
      }catch (Throwable e)
      {
      }
      if (session == null) {
        System.out.println("Fail to connect the worker: "+worker+". Continue cleaning");
        continue;
      }
      // IoSession session = future.getSession();
      session.write("shutdown").addListener(new IoFutureListener<WriteFuture>() {

        @Override
        public void operationComplete(WriteFuture future) {
          ParallelUtility.closeSession(future.getSession());
        }
      });
      System.out.println("Done");
    }
    ParallelUtility.unbind(acceptor);
    System.out.println("Bye");
  }

  protected class ServerHandler extends IoHandlerAdapter {

    // required in ParallelTest for instrument
    final Thread mainThread;

    public ServerHandler(Thread mainThread) {
      this.mainThread = mainThread;
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {
      cause.printStackTrace();
      ParallelUtility.closeSession(session);
    }

    @Override
    public void messageReceived(IoSession session, Object message) {
      if (message instanceof ExchangeTupleBatch) {
        System.out.println("message received by server: "+message);
        ExchangeTupleBatch tuples = (ExchangeTupleBatch) message;
        Server.this.inBuffer.get(tuples.getOperatorID()).offer(tuples);
      } else if (message instanceof String) {
        System.out.println("Query received by worker");
        Integer workerId = Server.this.workerIdToIndex.get(message);
        if (workerId != null) {
          Server.this.queryReceivedByWorker(workerId);
        }
      }

    }

    public void sessionIdle(IoSession session, IdleStatus status) {
      if (status.equals(IdleStatus.BOTH_IDLE)) {
        session.close(false);
      }
    }
  }

  protected void queryReceivedByWorker(int workerId) {
    workersReceivedQuery.add(workerId);
    if (workersReceivedQuery.size() >= this.workers.length) {
      ParallelUtility.broadcastMessageToWorkers("start", workers, this.minaHandler, -1);
      this.workersReceivedQuery.clear();
    }
  }

  final ConcurrentHashSet<Integer> workersReceivedQuery = new ConcurrentHashSet<Integer>();

  /**
   * Select the master worker for the coming query
   * */
  protected SocketInfo selectMasterWorker() {
    int master = (int) (Math.random() * this.workers.length);
    return this.workers[master];
  }

  public SocketInfo[] getWorkers() {
    return this.workers;
  }

  protected void dispatchWorkerQueryPlans(Map<SocketInfo, Operator> plans) {
    for (Map.Entry<SocketInfo, Operator> e : plans.entrySet()) {
      SocketInfo worker = e.getKey();
      Operator plan = e.getValue();
      IoSession ssss0 = ParallelUtility.createSession(worker.getAddress(), this.minaHandler, -1);
      // this session will be reused for the Workers to report the receive
      // of the queryplan, therefore, do not close it
      ssss0.write(plan);
    }
  }

  public void startServerQuery(CollectConsumer serverPlan) throws DbException
  {
    final LinkedBlockingQueue<_TupleBatch> buffer = new LinkedBlockingQueue<_TupleBatch>();
    serverPlan.setBuffer(buffer);
    Server.this.inBuffer.put(serverPlan.getOperatorID(), buffer);

    Schema td = serverPlan.getSchema();

    String names = "";
    for (int i = 0; i < td.numFields(); i++) {
      names += td.getFieldName(i) + "\t";
    }

    PrintStream out = null;
//    ByteArrayOutputStream b = null;
    out = System.out;

    out.println(names);
    for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
      out.print("-");
    }
    out.println("");

    serverPlan.open();
    int cnt = 0;
    while (serverPlan.hasNext()) {
      _TupleBatch tup = serverPlan.next();
      out.println(tup);
      cnt++;
    }
    out.println("\n " + cnt + " rows.");
//    if (b != null)
//      System.out.print(b.toString());

    serverPlan.close();
    Server.this.inBuffer.remove(serverPlan.getOperatorID());
  }
  
  protected void startQuery(ParallelQueryPlan queryPlan, SocketInfo masterWorker)
      throws DbException {
    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();
//    for (SocketInfo worker : this.workers) {
//      if (worker == masterWorker) {
//        workerPlans.put(worker, queryPlan.getMasterWorkerPlan());
//      } else
//        workerPlans.put(worker, queryPlan.getSlaveWorkerPlan());
//    }

    CollectConsumer serverPlan = queryPlan.getServerPlan();
    dispatchWorkerQueryPlans(workerPlans);
    startServerQuery(serverPlan);

  }

  protected void processQuery(Query q) throws DbException {
//    HashMap<String, Integer> aliasToId = q.getLogicalPlan().getTableAliasToIdMapping();
//    Operator sequentialQueryPlan = q.getPhysicalPlan();

    // if (sequentialQueryPlan != null) {
    // System.out.println("The sequential query plan is:");
    // OperatorCardinality.updateOperatorCardinality(
    // (Operator) sequentialQueryPlan, aliasToId, tableStats);
    // new QueryPlanVisualizer().printQueryPlanTree(sequentialQueryPlan,
    // System.out);
    // }

    SocketInfo masterWorker = selectMasterWorker();

    ParallelQueryPlan p = null;
//        ParallelQueryPlan.parallelizeQueryPlan(tid, sequentialQueryPlan, workers, masterWorker,
//            server, SingleFieldHashPartitionFunction.class);

    // p = ParallelQueryPlan.optimize(tid, p, new ProjectOptimizer(
    // new FilterOptimizer(
    // new AggregateOptimizer(new BloomFilterOptimizer(
    // workers, aliasToId, tableStats)))));

    startQuery(p, masterWorker);
  }

  public void processNextStatement(InputStream is) {
    // try {
    //
    // try {
    // ZqlParser p = new ZqlParser(is);
    // ZStatement s = p.readStatement();
    // Query q = null;
    //
    // if (s instanceof ZQuery)
    // q = handleQueryStatement((ZQuery) s, t.getId());
    // else {
    // System.err
    // .println("Currently only query statements (select) are supported");
    // return;
    // }
    // processQuery(q);
    //
    // } catch (Throwable a) {
    // // Whenever error happens, abort the current transaction
    //
    // if (t != null) {
    // t.abort();
    // System.out.println("Transaction " + t.getId().getId()
    // + " aborted because of unhandled error");
    // }
    //
    // if (a instanceof ParsingException
    // || a instanceof Zql.ParseException)
    // throw new ParsingException((Exception) a);
    // if (a instanceof Zql.TokenMgrError)
    // throw (Zql.TokenMgrError) a;
    // throw new DbException(a.getMessage());
    //
    //
    // } catch (DbException e) {
    // e.printStackTrace();
    // } catch (IOException e) {
    // e.printStackTrace();
    // } catch (simpledb.ParsingException e) {
    // System.out
    // .println("Invalid SQL expression: \n \t" + e.getMessage());
    // } catch (Zql.TokenMgrError e) {
    // System.out.println("Invalid SQL expression: \n \t " + e);
    //
    // }
    // }
  }
}