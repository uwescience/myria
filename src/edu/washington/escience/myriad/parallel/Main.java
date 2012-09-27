package edu.washington.escience.myriad.parallel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.parallel.ConcurrentInMemoryTupleBatch;
import edu.washington.escience.myriad.parallel.JdbcTupleBatch;
import edu.washington.escience.myriad.parallel.SQLiteTupleBatch;
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
import edu.washington.escience.myriad.parallel.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.JdbcQueryScan;
import edu.washington.escience.myriad.parallel.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.Operator;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.SocketInfo;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

/**
 * Runs some simple tests.
 * 
 * @author dhalperi, slxu
 * 
 */
public class Main {
  public static void main(String[] args) throws Exception{
    // JdbcTest();
    // SQLiteTest();
    // sqliteEmptyTest();
    // parallelTestJDBC(args);
    // parallelTestSQLite(args);
    // jdbcTest_slxu(args);
    // shuffleTestSQLite(args);
//    sqliteInsertSpeedTest();
    filesystemWriteTest();
    //shuffleTestSQLite(args);
    dupElimTestSQLite(args);
  };
  
  public static void filesystemWriteTest() throws Exception 
  {
    Date now = new Date();
    Date begin = now;
    Random r = new Random();
    File f = new File("/tmp/tmpfile");
    FileOutputStream fos = new FileOutputStream(f);
    
    for (int i = 0; i < 1000000; i++) {
      fos.write((i+"|"+i + "th " + r.nextInt()).getBytes());
    }
    fos.close();
    System.out.println((new Date().getTime() - begin.getTime()) * 1.0 / 1000 + " seconds in total");
    //2.371 seconds
  }

  public static void sqliteInsertSpeedTest() throws SQLiteException {
    SQLiteConnection sqliteConnection = new SQLiteConnection(new File("/tmp/test/test.db"));
    sqliteConnection.open(false);

    /* Set up and execute the query */
    SQLiteStatement statement = sqliteConnection.prepare("insert into test (id,name) values (?,?)");

    Date now = new Date();
    Date begin = now;
    Random r = new Random();
    for (int i = 0; i < 1000000; i++) {
      if (i % 100 ==0)
        sqliteConnection.exec("begin transaction");
      statement.bind(1, i);
      statement.bind(2, i + "th " + r.nextInt());
      statement.step();
      statement.reset();
      if (i % 1000 == 0) {
        Date tmp = new Date();
        System.out.println((tmp.getTime() - now.getTime()) * 1.0 / 1000 + " seconds per 1000");
        now = tmp;
      }
      if (i % 100 ==99)
        sqliteConnection.exec("commit transaction");
    }
    System.out.println((new Date().getTime() - begin.getTime()) * 1.0 / 1000 + " seconds in total");

    // 4 seconds for 1000000 tuples insert in one transaction
    // 93.884 seconds for 1000000 tuples insert in 1000-size tuplebatches.
  }

  public static void sqliteEmptyTest() throws SQLiteException {
    SQLiteConnection sqliteConnection = new SQLiteConnection(new File("/tmp/test/emptytable.db"));
    sqliteConnection.open(false);

    /* Set up and execute the query */
    SQLiteStatement statement = sqliteConnection.prepare("select * from empty");

    /* Step the statement once so we can figure out the Schema */
    statement.step();
    try {
      if (!statement.hasStepped()) {
        statement.step();
      }
      System.out.println(Schema.fromSQLiteStatement(statement));
    } catch (SQLiteException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static class DoNothingOperator extends Operator {

    Operator[] children;
    Schema outputSchema;

    public DoNothingOperator(Schema outputSchema, Operator[] children) {
      this.outputSchema = outputSchema;
      this.children = children;
    }

    @Override
    protected _TupleBatch fetchNext() throws DbException {
      if (children != null) {
        for (Operator child : children) {
          while (child.hasNext())
            child.next();
        }
      }
      return null;
    }

    @Override
    public Operator[] getChildren() {
      return this.children;
    }

    @Override
    public Schema getSchema() {
      return this.outputSchema;
    }

    @Override
    public void setChildren(Operator[] children) {
      this.children = children;
    }

    public void open() throws DbException {
      if (children != null) {
        for (Operator child : children)
          child.open();
      }
      super.open();
    }

  }
  
  public static void dupElimTestSQLite(final String[] args) throws DbException, IOException {
    Configuration conf = new Configuration();
    SocketInfo[] workers = conf.getWorkers();
    SocketInfo server = conf.getServer();

    ExchangePairID serverReceiveID = ExchangePairID.newID();
    ExchangePairID shuffle1ID = ExchangePairID.newID();
    ExchangePairID shuffle2ID = ExchangePairID.newID();

    Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] table1ColumnNames = new String[] { "id", "name" };
    Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] table2ColumnNames = new String[] { "id", "name" };
    Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] outputColumnNames = new String[] { "id", "name" };
    Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    int numPartition = 2;

    PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable.db", "select * from testtable1", tableSchema1);
    //ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, workers, pf);

    SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable.db", "select * from testtable2", tableSchema2);
    //ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, workers, pf);

    //SQLiteTupleBatch bufferWorker1 = new SQLiteTupleBatch(tableSchema1, "temptable.db", "temptable1");
    //ShuffleConsumer sc1 = new ShuffleConsumer(sp1, shuffle1ID, workers, bufferWorker1);

    //SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(tableSchema2, "temptable.db", "temptable2");
    //ShuffleConsumer sc2 = new ShuffleConsumer(sp2, shuffle2ID, workers, bufferWorker2);

    //SQLiteSQLProcessor ssp =
    //    new SQLiteSQLProcessor("testtable.db",
    //        "select * from testtable1 union select * from testtable2", outputSchema,
    //        new Operator[] { scan1, scan2 });

    //DoNothingOperator dno = new DoNothingOperator(outputSchema, new Operator[] { sc1, sc2 });

    //CollectProducer cp = new CollectProducer(ssp, serverReceiveID, server.getAddress());
    DupElim dupElim1 = new DupElim(tableSchema1, scan1);
    DupElim dupElim2 = new DupElim(tableSchema2, scan2);
    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();    
    workerPlans.put(workers[0], new CollectProducer(dupElim1, serverReceiveID, server.getAddress()));
    workerPlans.put(workers[1], new CollectProducer(dupElim2, serverReceiveID, server.getAddress()));

    OutputStreamSinkTupleBatch serverBuffer = new OutputStreamSinkTupleBatch(outputSchema, System.out);

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
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID, workers, serverBuffer));

  }

  public static void shuffleTestSQLite(final String[] args) throws DbException, IOException {
    Configuration conf = new Configuration();
    SocketInfo[] workers = conf.getWorkers();
    SocketInfo server = conf.getServer();

    ExchangePairID serverReceiveID = ExchangePairID.newID();
    ExchangePairID shuffle1ID = ExchangePairID.newID();
    ExchangePairID shuffle2ID = ExchangePairID.newID();

    Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] table1ColumnNames = new String[] { "id", "name" };
    Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] table2ColumnNames = new String[] { "id", "name" };
    Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    int numPartition = 2;

    PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable.db", "select * from testtable1", tableSchema1);
    ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, workers, pf);

    SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable.db", "select * from testtable2", tableSchema2);
    ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, workers, pf);

    SQLiteTupleBatch bufferWorker1 = new SQLiteTupleBatch(tableSchema1, "temptable.db", "temptable1");
    ShuffleConsumer sc1 = new ShuffleConsumer(sp1, shuffle1ID, workers, bufferWorker1);

    SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(tableSchema2, "temptable.db", "temptable2");
    ShuffleConsumer sc2 = new ShuffleConsumer(sp2, shuffle2ID, workers, bufferWorker2);

    SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("temptable.db",
            "select * from temptable1 inner join temptable2 on temptable1.name=temptable2.name", outputSchema,
            new Operator[] { sc1, sc2 });

    DoNothingOperator dno = new DoNothingOperator(outputSchema, new Operator[] { sc1, sc2 });

    CollectProducer cp = new CollectProducer(ssp, serverReceiveID, server.getAddress());

    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();
    workerPlans.put(workers[0], cp);
    workerPlans.put(workers[1], cp);

    OutputStreamSinkTupleBatch serverBuffer = new OutputStreamSinkTupleBatch(outputSchema, System.out);

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
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID, workers, serverBuffer));

  }
  
  public static void parallelTestSQLite(final String[] args) throws DbException, IOException {
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

    // String username = "root";
    // String password = "1234";

    Configuration conf = new Configuration();

    // InetSocketAddress worker1 = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 9001);
    // InetSocketAddress worker2 = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 9002);
    // InetSocketAddress server = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 8001);
    SocketInfo[] workers = conf.getWorkers();
    SocketInfo server = conf.getServer();

    ExchangePairID serverReceiveID = ExchangePairID.newID();
    ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    Type[] types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    String[] columnNames = new String[] { "id", "name" };
    Schema outputSchema = new Schema(types, columnNames);

    SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable1.db", "select distinct * from testtable1", outputSchema);
    CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, workers[1].getAddress());

    SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable2.db", "select distinct * from testtable2", outputSchema);
    CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, workers[1].getAddress());
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(outputSchema, "/tmp/temptable1.db", "temptable1");
    CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, workers, bufferWorker2);
    SQLiteSQLProcessor scan22 =
        new SQLiteSQLProcessor("temptable1.db", "select distinct * from temptable1", outputSchema,
            new Operator[] { cc2 });
    CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, server.getAddress());
    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();
    workerPlans.put(workers[0], cp1);
    workerPlans.put(workers[1], cp22);

    OutputStreamSinkTupleBatch serverBuffer = new OutputStreamSinkTupleBatch(outputSchema, System.out);

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
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID,
        new SocketInfo[] { workers[1] }, serverBuffer));
  }

  public static void parallelTestJDBC(final String[] args) throws DbException, IOException {
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

    Configuration conf = new Configuration();

    // InetSocketAddress worker1 = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 9001);
    // InetSocketAddress worker2 = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 9002);
    // InetSocketAddress server = new InetSocketAddress("slxu-csuw-desktop.cs.washington.edu", 8001);
    SocketInfo[] workers = conf.getWorkers();
    SocketInfo server = conf.getServer();

    ExchangePairID serverReceiveID = ExchangePairID.newID();
    ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    Type[] types = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
    String[] columnNames = new String[] { "id", "name" };
    Schema outputSchema = new Schema(types, columnNames);

    JdbcQueryScan scan1 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable1", outputSchema, username, password);
    CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, workers[1].getAddress());

    JdbcQueryScan scan2 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable2", outputSchema, username, password);
    CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, workers[1].getAddress());
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    JdbcTupleBatch bufferWorker2 =
        new JdbcTupleBatch(outputSchema, "temptable1", "jdbc:mysql://localhost:3306/test", "com.mysql.jdbc.Driver",
            username, password);
    CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, workers, bufferWorker2);
    JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from temptable1", outputSchema, cc2, username, password);
    CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, server.getAddress());
    HashMap<SocketInfo, Operator> workerPlans = new HashMap<SocketInfo, Operator>();
    workerPlans.put(workers[0], cp1);
    workerPlans.put(workers[1], cp22);

    OutputStreamSinkTupleBatch serverBuffer = new OutputStreamSinkTupleBatch(outputSchema, System.out);

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
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID,
        new SocketInfo[] { workers[1] }, serverBuffer));
  }

  public static void jdbcTest_slxu(String[] args) throws NoSuchElementException, DbException {

    Schema outputSchema = new Schema(new Type[] { Type.INT_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });
    JdbcQueryScan scan =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test", "select * from testtable1",
            outputSchema, "", "");
    // Select filter1 = new Select(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);
    //
    // Select filter2 = new Select(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

    // ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    // fieldIdx.add(1);
    // ArrayList<Type> fieldType = new ArrayList<Type>();
    // fieldType.add(Type.STRING_TYPE);

    // Project project = new Project(fieldIdx, fieldType, filter2);

    Operator root = scan;

    root.open();
    // scan.open();

    // Schema schema = root.getSchema();

    // if (schema != null) {
    // System.out.println(schema);
    // } else
    // return;

    while (root.hasNext()) {
      _TupleBatch tb = root.next();

      System.out.println(tb.outputRawData());
    }
  }

  public static void SQLiteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "SELECT * FROM testtable";
    final String insert = "INSERT INTO testtable2 VALUES(?)";

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    Schema outputSchema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    /* Scan the testtable in database */
    SQLiteQueryScan scan = new SQLiteQueryScan(filename, query, outputSchema);

    /* Filter on first column INTEGER >= 50 */
    // Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Long(50), scan);
    /* Filter on first column INTEGER <= 60 */
    // Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Long(60), filter1);

    /* Project onto second column STRING */
    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);
    // Project project = new Project(fieldIdx, fieldType, filter2);

    /* Project is the output operator */
    Operator root = scan;
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
      // SQLiteAccessMethod.tupleBatchInsert(filename, insert, (TupleBatch) tb);
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
    JdbcQueryScan scan = new JdbcQueryScan(jdbcDriverName, connectionString, query, schema, "", "");
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
      JdbcAccessMethod.tupleBatchInsert(jdbcDriverName, connectionString, insert, (TupleBatch) tb, "", "");
    }

    root.close();
  }

  /** Inaccessible. */
  private Main() {
  }
}
