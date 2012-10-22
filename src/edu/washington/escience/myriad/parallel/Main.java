package edu.washington.escience.myriad.parallel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.operator.BlockingDataReceiver;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.Filter;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.JdbcSQLProcessor;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Runs some simple tests.
 * 
 * @author dhalperi, slxu
 * 
 */
public final class Main {
  public static final int MASTER_ID = 0;
  public static final int WORKER_1_ID = 1;

  public static final int WORKER_2_ID = 2;;

  public static void joinTestSQLite(final String[] args) throws DbException, IOException {
    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table1ColumnNames = new String[] { "id", "name" };
    final Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table2ColumnNames = new String[] { "id", "name" };
    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    final Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    final Schema outputSchema = tableSchema1;
    final Schema joinSchema = new Schema(outputTypes, outputColumnNames);
    final int numPartition = 2;

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable1", tableSchema1);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable1.db", "select * from testtable1", tableSchema2);

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by id

    final int numIter = 5;
    final ShuffleProducer sp1[] = new ShuffleProducer[numIter];
    final ShuffleProducer sp2[] = new ShuffleProducer[numIter];
    final ShuffleConsumer sc1[] = new ShuffleConsumer[numIter];
    final ShuffleConsumer sc2[] = new ShuffleConsumer[numIter];
    final LocalJoin localjoin[] = new LocalJoin[numIter];
    final Project proj[] = new Project[numIter];
    final DupElim dupelim[] = new DupElim[numIter];
    final SQLiteQueryScan scan[] = new SQLiteQueryScan[numIter];
    ExchangePairID arrayID1, arrayID2;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    sp1[0] = new ShuffleProducer(scan1, arrayID1, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);
    sp2[0] = new ShuffleProducer(scan2, arrayID2, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);

    for (int i = 1; i < numIter; ++i) {
      sc1[i] = new ShuffleConsumer(sp1[i - 1], arrayID1, new int[] { WORKER_1_ID, WORKER_2_ID });
      sc2[i] = new ShuffleConsumer(sp2[i - 1], arrayID2, new int[] { WORKER_1_ID, WORKER_2_ID });
      localjoin[i] = new LocalJoin(joinSchema, sc1[i], sc2[i], new int[] { 0 }, new int[] { 0 });
      proj[i] = new Project(new Integer[] { 0, 3 }, localjoin[i]);
      dupelim[i] = new DupElim(proj[i]);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();
      scan[i] = new SQLiteQueryScan("testtable" + (i + 1) + ".db", "select * from testtable1", tableSchema1);
      sp1[i] = new ShuffleProducer(scan[i], arrayID1, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);
      sp2[i] = new ShuffleProducer(dupelim[i], arrayID2, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);
    }
    final CollectProducer cp = new CollectProducer(dupelim[numIter - 1], serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp);
    workerPlans.put(WORKER_2_ID, cp);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    Server.runningInstance.exchangeSchema.put(serverReceiveID, outputSchema);
    final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_1_ID, WORKER_2_ID });
    serverPlan.setInputBuffer(buffer);
    Server.runningInstance.dataBuffer.put(serverPlan.getOperatorID(), buffer);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(serverPlan);
  }

  public static void localJoinTestSQLite(final String[] args) throws DbException, IOException {
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table1ColumnNames = new String[] { "id", "name" };
    final Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table2ColumnNames = new String[] { "id", "name" };
    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    final Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    final int numPartition = 2;

    // final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    // pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable1.db", "select * from testtable1", tableSchema1);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable2.db", "select * from testtable2", tableSchema2);

    final LocalJoin localjoin = new LocalJoin(outputSchema, scan1, scan2, new int[] { 0 }, new int[] { 0 });
    // final LocalJoin localjoin2 = new LocalJoin(outputSchema, scan1, scan2, new int[] { 0 }, new int[] { 0 });

    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final CollectProducer cp2 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    // final CollectConsumer cc1 = new CollectConsumer(cp1, collectID, new int[] { WORKER_1_ID, WORKER_2_ID });
    // final DupElim dumElim3 = new DupElim(tableSchema1, cc1);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp1);// new CollectProducer(dumElim3, serverReceiveID, MASTER_ID));
    // workerPlans.put(WORKER_2_ID, cp2);// new CollectProducer(dupElim2, collectID, WORKER_1_ID));

    // OutputStreamSinkTupleBatch serverBuffer = new OutputStreamSinkTupleBatch(outputSchema, System.out);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    Server.runningInstance.exchangeSchema.put(serverReceiveID, outputSchema);
    final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
    final CollectConsumer serverPlan = new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_1_ID });// ,
                                                                                                                     // WORKER_2_ID
                                                                                                                     // });
    serverPlan.setInputBuffer(buffer);
    Server.runningInstance.dataBuffer.put(serverPlan.getOperatorID(), buffer);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(serverPlan);

    // Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    // System.out.println("Query dispatched to the workers");
    // Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID, new int[] { 1, 2 }));

  }

  public static void dupElimTestSQLite(final String[] args) throws DbException, IOException {
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID collectID = ExchangePairID.newID();

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table1ColumnNames = new String[] { "id", "name" };
    final Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table2ColumnNames = new String[] { "id", "name" };
    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name" };
    final Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    final Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testdupelim.db", "select * from testtable1", tableSchema1);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testdupelim.db", "select * from testtable1", tableSchema2);
    final DupElim dupElim1 = new DupElim(scan1);
    final DupElim dupElim2 = new DupElim(scan2);
    final CollectProducer cp1 = new CollectProducer(dupElim1, serverReceiveID, MASTER_ID);
    final CollectProducer cp2 = new CollectProducer(dupElim2, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp1);
    workerPlans.put(WORKER_2_ID, cp2);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    Server.runningInstance.exchangeSchema.put(serverReceiveID, outputSchema);
    final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_1_ID, WORKER_2_ID });
    serverPlan.setInputBuffer(buffer);
    Server.runningInstance.dataBuffer.put(serverPlan.getOperatorID(), buffer);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(serverPlan);
  }

  public static void filesystemWriteTest() throws Exception {
    final Date now = new Date();
    final Date begin = now;
    final Random r = new Random();
    final File f = new File("/tmp/tmpfile");
    final FileOutputStream fos = new FileOutputStream(f);

    for (int i = 0; i < 1000000; i++) {
      fos.write((i + "|" + i + "th " + r.nextInt()).getBytes());
    }
    fos.close();
    System.out.println((new Date().getTime() - begin.getTime()) * 1.0 / 1000 + " seconds in total");
    // 2.371 seconds
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
    final Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });
    final String connectionString =
        "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName + "?user=" + user + "&password=" + password;
    final JdbcQueryScan scan = new JdbcQueryScan(jdbcDriverName, connectionString, query, schema, "", "");
    final Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);

    final Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

    final ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    final ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);

    final Project project = new Project(fieldIdx, fieldType, filter2);

    final Operator root = project;

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
      final _TupleBatch tb = root.next();
      System.out.println(tb);
      JdbcAccessMethod.tupleBatchInsert(jdbcDriverName, connectionString, insert, (TupleBatch) tb, "", "");
    }

    root.close();
  }

  public static void jdbcTest_slxu(final String[] args) throws NoSuchElementException, DbException {

    final Schema outputSchema =
        new Schema(new Type[] { Type.INT_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });
    final JdbcQueryScan scan =
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

    final Operator root = scan;

    root.open();
    // scan.open();

    // Schema schema = root.getSchema();

    // if (schema != null) {
    // System.out.println(schema);
    // } else
    // return;

    while (root.hasNext()) {
      final _TupleBatch tb = root.next();

      System.out.println(tb.outputRawData());
    }
  }

  public static void main(final String[] args) throws Exception {
    // JdbcTest();
    // SQLiteTest();
    // sqliteEmptyTest();
    // parallelTestJDBC(args);
    // parallelTestSQLite(args);
    // jdbcTest_slxu(args);
    // shuffleTestSQLite(args);
    // sqliteInsertSpeedTest();
    // filesystemWriteTest();
    // shuffleTestSQLite(args);
    // dupElimTestSQLite(args);
    // localJoinTestSQLite(args);
    joinTestSQLite(args);
    // shuffleTestSQLite(args);
    // sqliteInsertSpeedTest();
    // filesystemWriteTest();
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

    final String username = "root";
    final String password = "1234";

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final Type[] types = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
    final String[] columnNames = new String[] { "id", "name" };
    final Schema outputSchema = new Schema(types, columnNames);

    final JdbcQueryScan scan1 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable1", outputSchema, username, password);
    final CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, WORKER_2_ID);

    final JdbcQueryScan scan2 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable2", outputSchema, username, password);
    final CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, WORKER_2_ID);
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    final JdbcTupleBatch bufferWorker2 =
        new JdbcTupleBatch(outputSchema, "temptable1", "jdbc:mysql://localhost:3306/test", "com.mysql.jdbc.Driver",
            username, password);
    final CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, new int[] { 1, 2 });
    final BlockingDataReceiver block2 = new BlockingDataReceiver(bufferWorker2, cc2);
    final JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from temptable1", outputSchema, block2, username, password);
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp1);
    workerPlans.put(WORKER_2_ID, cp22);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID,
        new int[] { WORKER_2_ID }));
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

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final Type[] types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] columnNames = new String[] { "id", "name" };
    final Schema outputSchema = new Schema(types, columnNames);

    final SQLiteQueryScan scan1 =
        new SQLiteQueryScan("testtable1.db", "select distinct * from testtable1", outputSchema);
    final CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, WORKER_2_ID);

    final SQLiteQueryScan scan2 =
        new SQLiteQueryScan("testtable2.db", "select distinct * from testtable2", outputSchema);
    final CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, WORKER_2_ID);
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    final SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(outputSchema, "/tmp/temptable1.db", "temptable1");
    final CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, new int[] { WORKER_1_ID, WORKER_2_ID });
    final BlockingDataReceiver block2 = new BlockingDataReceiver(bufferWorker2, cc2);
    final SQLiteSQLProcessor scan22 =
        new SQLiteSQLProcessor("temptable1.db", "select distinct * from temptable1", outputSchema,
            new Operator[] { block2 });
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp1);
    workerPlans.put(WORKER_2_ID, cp22);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(new CollectConsumer(outputSchema, serverReceiveID, new int[] { 2 }));
  }

  public static void shuffleTestSQLite(final String[] args) throws DbException, IOException {
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID shuffle1ID = ExchangePairID.newID();
    final ExchangePairID shuffle2ID = ExchangePairID.newID();

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table1ColumnNames = new String[] { "id", "name" };
    final Type[] table2Types = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] table2ColumnNames = new String[] { "id", "name" };
    final Type[] outputTypes = new Type[] { Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE };
    final String[] outputColumnNames = new String[] { "id", "name", "id", "name" };
    final Schema tableSchema1 = new Schema(table1Types, table1ColumnNames);
    final Schema tableSchema2 = new Schema(table2Types, table2ColumnNames);
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable.db", "select * from testtable1", tableSchema1);
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);

    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable.db", "select * from testtable2", tableSchema2);
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, new int[] { WORKER_1_ID, WORKER_2_ID }, pf);

    final SQLiteTupleBatch bufferWorker1 = new SQLiteTupleBatch(tableSchema1, "temptable.db", "temptable1");
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1, shuffle1ID, new int[] { WORKER_1_ID, WORKER_2_ID });
    final BlockingDataReceiver buffer1 = new BlockingDataReceiver(bufferWorker1, sc1);

    final SQLiteTupleBatch bufferWorker2 = new SQLiteTupleBatch(tableSchema2, "temptable.db", "temptable2");
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2, shuffle2ID, new int[] { WORKER_1_ID, WORKER_2_ID });
    final BlockingDataReceiver buffer2 = new BlockingDataReceiver(bufferWorker2, sc2);

    final SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("temptable.db",
            "select * from temptable1 inner join temptable2 on temptable1.name=temptable2.name", outputSchema,
            new Operator[] { buffer1, buffer2 });

    // DoNothingOperator dno = new DoNothingOperator(outputSchema, new Operator[] { buffer1, buffer2 });

    final CollectProducer cp = new CollectProducer(ssp, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_1_ID, cp);
    workerPlans.put(WORKER_2_ID, cp);

    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(args);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }
    Server.runningInstance.exchangeSchema.put(serverReceiveID, outputSchema);
    final LinkedBlockingQueue<ExchangeTupleBatch> buffer = new LinkedBlockingQueue<ExchangeTupleBatch>();
    final CollectConsumer serverPlan =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_1_ID, WORKER_2_ID });
    serverPlan.setInputBuffer(buffer);
    Server.runningInstance.dataBuffer.put(serverPlan.getOperatorID(), buffer);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(serverPlan);

  }

  public static void sqliteEmptyTest() throws SQLiteException {
    final SQLiteConnection sqliteConnection = new SQLiteConnection(new File("/tmp/test/emptytable.db"));
    sqliteConnection.open(false);

    /* Set up and execute the query */
    final SQLiteStatement statement = sqliteConnection.prepare("select * from empty");

    /* Step the statement once so we can figure out the Schema */
    statement.step();
    try {
      if (!statement.hasStepped()) {
        statement.step();
      }
      System.out.println(Schema.fromSQLiteStatement(statement));
    } catch (final SQLiteException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static void sqliteInsertSpeedTest() throws SQLiteException {
    final SQLiteConnection sqliteConnection = new SQLiteConnection(new File("/tmp/test/test.db"));
    sqliteConnection.open(false);

    /* Set up and execute the query */
    final SQLiteStatement statement = sqliteConnection.prepare("insert into test (id,name) values (?,?)");

    Date now = new Date();
    final Date begin = now;
    final Random r = new Random();
    for (int i = 0; i < 1000000; i++) {
      if (i % 100 == 0) {
        sqliteConnection.exec("begin transaction");
      }
      statement.bind(1, i);
      statement.bind(2, i + "th " + r.nextInt());
      statement.step();
      statement.reset();
      if (i % 1000 == 0) {
        final Date tmp = new Date();
        System.out.println((tmp.getTime() - now.getTime()) * 1.0 / 1000 + " seconds per 1000");
        now = tmp;
      }
      if (i % 100 == 99) {
        sqliteConnection.exec("commit transaction");
      }
    }
    System.out.println((new Date().getTime() - begin.getTime()) * 1.0 / 1000 + " seconds in total");

    // 4 seconds for 1000000 tuples insert in one transaction
    // 93.884 seconds for 1000000 tuples insert in 1000-size tuplebatches.
  }

  public static void SQLiteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "SELECT * FROM testtable";

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    final Schema outputSchema =
        new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    /* Scan the testtable in database */
    final SQLiteQueryScan scan = new SQLiteQueryScan(filename, query, outputSchema);

    /* Filter on first column INTEGER >= 50 */
    // Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Long(50), scan);
    /* Filter on first column INTEGER <= 60 */
    // Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Long(60), filter1);

    /* Project onto second column STRING */
    final ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    final ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);
    // Project project = new Project(fieldIdx, fieldType, filter2);

    /* Project is the output operator */
    final Operator root = scan;
    root.open();

    /* For debugging purposes, print Schema */
    final Schema schema = root.getSchema();
    if (schema != null) {
      System.out.println("Schema of result is: " + schema);
    } else {
      System.err.println("Result has no Schema, exiting");
      root.close();
      return;
    }

    /* Print all the results */
    while (root.hasNext()) {
      final _TupleBatch tb = root.next();
      System.out.println(tb);
      // SQLiteAccessMethod.tupleBatchInsert(filename, insert, (TupleBatch) tb);
    }

    /* Cleanup */
    root.close();
  }

  /** Inaccessible. */
  private Main() {
  }
}
