package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.washington.escience.myriad.parallel.ConcurrentInMemoryTupleBatch;
import edu.washington.escience.myriad.parallel.JdbcTupleBatch;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.DbException;
import edu.washington.escience.myriad.parallel.JdbcSQLProcessor;
import edu.washington.escience.myriad.parallel.JdbcQueryScan;
import edu.washington.escience.myriad.parallel.Operator;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Exchange.ParallelOperatorID;

/**
 * Runs some simple tests.
 * 
 * @author dhalperi, slxu
 * 
 */
public class Main {
  public static void main(String[] args) throws NoSuchElementException, DbException, IOException {
    // JdbcTest();
    // SQLiteTest();
    parallelTest(args);
//    jdbcTest_slxu(args);
  };

  public static void parallelTest(final String[] args) throws DbException, IOException {
    // create table testtable1 (id int, name varchar(20));
    // insert into testtable1 (id,name) values (1,'name1'), (2, 'name2');
    //
    // create table testtable2 (id int, name varchar(20));
    // insert into testtable2 (id,name) values (1,'name1'), (2, 'name2');
    //
    // create table temptable1 (id int, name varchar(20));

    // Process worker1P = new
    // ProcessBuilder("/usr/bin/java","-Dfile.encoding=UTF-8 -classpath /home/slxu/workspace/JdbcAccessMethod/bin:/home/slxu/workspace/JdbcAccessMethod/lib/mysql-connector-java-5.1.21-bin.jar:/home/slxu/workspace/JdbcAccessMethod/lib/sqlite4java-282/sqlite4java.jar:/home/slxu/workspace/JdbcAccessMethod/lib/guava-12.0.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-core-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-filter-compression-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-api-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-log4j12-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/log4j-1.2.17.jar:/home/slxu/workspace/JdbcAccessMethod/lib/jline-0.9.94.jar:/home/slxu/workspace/JdbcAccessMethod/lib/commons-lang3-3.1.jar edu.washington.escience.parallel.Worker localhost:9001 localhost:8001").start();
    // Process worker2P = new ProcessBuilder("java",
    // "-Dfile.encoding=UTF-8 -classpath /home/slxu/workspace/JdbcAccessMethod/bin:/home/slxu/workspace/JdbcAccessMethod/lib/mysql-connector-java-5.1.21-bin.jar:/home/slxu/workspace/JdbcAccessMethod/lib/sqlite4java-282/sqlite4java.jar:/home/slxu/workspace/JdbcAccessMethod/lib/guava-12.0.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-core-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-filter-compression-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-api-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-log4j12-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/log4j-1.2.17.jar:/home/slxu/workspace/JdbcAccessMethod/lib/jline-0.9.94.jar:/home/slxu/workspace/JdbcAccessMethod/lib/commons-lang3-3.1.jar edu.washington.escience.parallel.Worker localhost:9002 localhost:8001").start();

    String username = "root";
    String password = "1234";
    
    InetSocketAddress worker1 = new InetSocketAddress("carlise.cs.washington.edu", 9001);
    InetSocketAddress worker2 = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 9002);
    InetSocketAddress server = new InetSocketAddress("carlise.cs.washington.edu", 8001);
    SocketInfo[] workers =
        new SocketInfo[] {
            new SocketInfo(worker1.getHostString(), worker1.getPort()),
            new SocketInfo(worker2.getHostString(), worker2.getPort()) };

    ParallelOperatorID serverReceiveID = ParallelOperatorID.newID();
    ParallelOperatorID worker2ReceiveID = ParallelOperatorID.newID();

    Type[] types = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
    String[] columnNames = new String[] { "id", "name" };
    Schema outputSchema = new Schema(types, columnNames);

    JdbcQueryScan scan1 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable1", outputSchema,username,password);
    CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, worker2);

    JdbcQueryScan scan2 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable2", outputSchema,username,password);
    CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, worker2);
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    JdbcTupleBatch bufferWorker2 =
        new JdbcTupleBatch(outputSchema, "temptable1", "jdbc:mysql://localhost:3306/test", "com.mysql.jdbc.Driver",username,password);
    CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, workers, bufferWorker2);
    JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from temptable1", outputSchema, cc2,username,password);
    CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, server);
    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();
    workerPlans.put(workers[0], cp1);
    workerPlans.put(workers[1], cp22);

    ConcurrentInMemoryTupleBatch serverBuffer = new ConcurrentInMemoryTupleBatch(outputSchema);

    new Thread() {
      public void run() {
        try {
          Server.main(args);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null)
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID , new SocketInfo[]{workers[1]}, serverBuffer));
  }

  public static void jdbcTest_slxu(String[] args) throws NoSuchElementException, DbException {

    Schema outputSchema = new Schema(new Type[]{Type.INT_TYPE,Type.STRING_TYPE}, new String[]{"id","name"});
    JdbcQueryScan scan =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select * from testtable1", outputSchema,"","");
//    Select filter1 = new Select(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);
//
//    Select filter2 = new Select(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

//    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
//    fieldIdx.add(1);
//    ArrayList<Type> fieldType = new ArrayList<Type>();
//    fieldType.add(Type.STRING_TYPE);

//    Project project = new Project(fieldIdx, fieldType, filter2);

    Operator root = scan;

    root.open();
//    scan.open();

//    Schema schema = root.getSchema();

//    if (schema != null) {
//      System.out.println(schema);
//    } else
//      return;

    while (root.hasNext())
    {
      _TupleBatch tb = root.next();
      
      System.out.println(tb.outputRawData());
    }
  }
  
  public static void SQLiteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "SELECT * FROM testtable";
    final String insert = "INSERT INTO testtable2 VALUES(?)";

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    /* Scan the testtable in database */
    SQLiteQueryScan scan = new SQLiteQueryScan(filename, query);

    /* Filter on first column INTEGER >= 50 */
    Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Long(50), scan);
    /* Filter on first column INTEGER <= 60 */
    Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Long(60), filter1);

    /* Project onto second column STRING */
    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);
    Project project = new Project(fieldIdx, fieldType, filter2);

    /* Project is the output operator */
    Operator root = project;
    root.open();

    /* For debugging purposes, print Schema */
    Schema schema = root.getSchema();
    if (schema != null) {
      System.out.println("Schema of result is: " + schema);
    } else {
      System.err.println("Result has no Schema, exiting");
      root.close();
      return;
    }

    /* Print all the results */
    while (root.hasNext()) {
      _TupleBatch tb = root.next();
      System.out.println(tb);
      SQLiteAccessMethod.tupleBatchInsert(filename, insert, (TupleBatch) tb);
    }

    /* Cleanup */
    root.close();
  }

  public static void JdbcTest() throws DbException {
    final String host = "54.245.108.198";
    final int port = 3306;
    final String user = "myriad";
    final String password = "nays26[shark";
    final String dbms = "mysql";
    final String databaseName = "myriad_test";
    final String jdbcDriverName = "com.mysql.jdbc.Driver";
    final String query = "select * from testtable";
    final String insert = "INSERT INTO testtable2 VALUES(?)";
    Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });
    String connectionString =
        "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName + "?user=" + user + "&password=" + password;
    JdbcQueryScan scan = new JdbcQueryScan(jdbcDriverName, connectionString, query, schema,"","");
    Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);

    Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);

    Project project = new Project(fieldIdx, fieldType, filter2);

    Operator root = project;

    root.open();

    // Schema schema = root.getSchema();

    // if (schema != null)
    // {
    // System.out.println("Schema of result is: " + schema);
    // } else {
    // System.err.println("Result has no Schema, exiting");
    // root.close();
    // return;
    // }

    while (root.hasNext()) {
      _TupleBatch tb = root.next();
      System.out.println(tb);
      JdbcAccessMethod.tupleBatchInsert(jdbcDriverName, connectionString, insert, (TupleBatch) tb,"","");
    }

    root.close();
  }

  /** Inaccessible. */
  private Main() {
  }
}
