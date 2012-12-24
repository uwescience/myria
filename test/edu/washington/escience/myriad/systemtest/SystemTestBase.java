package edu.washington.escience.myriad.systemtest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.Worker;
import edu.washington.escience.myriad.tool.EclipseClasspathReader;
import edu.washington.escience.myriad.util.FSUtils;
import edu.washington.escience.myriad.util.SQLiteUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class SystemTestBase {
  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static class Tuple implements Comparable<Tuple> {
    Comparable[] values;

    public Tuple(final int numFields) {
      values = new Comparable[numFields];
    }

    @Override
    public int compareTo(final Tuple o) {
      if (o == null || o.values.length != values.length) {
        throw new IllegalArgumentException("invalid tuple");
      }
      for (int i = 0; i < values.length; i++) {
        final int sub = values[i].compareTo(o.values[i]);
        if (sub != 0) {
          return sub;
        }
      }
      return 0;
    }

    @Override
    public boolean equals(final Object o) {
      return compareTo((Tuple) o) == 0;
    }

    public Object get(final int i) {
      return values[i];
    }

    @Override
    public int hashCode() {
      int h = 1;
      for (final Comparable o : values) {
        h = 31 * h + o.hashCode();
      }
      return h;
    }

    public int numFields() {
      return values.length;
    }

    public void set(final int i, final Comparable v) {
      values[i] = v;
    }

    public void setAll(final int start, final Tuple other) {
      System.arraycopy(other.values, 0, values, start, other.values.length);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < values.length - 1; i++) {
        Comparable<?> v = values[i];
        sb.append(v + ", ");
      }
      sb.append(values[values.length - 1] + ")");
      return sb.toString();
    }
  }

  public static final Schema JOIN_INPUT_SCHEMA = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE },
      new String[] { "id", "name" });
  public static final String JOIN_TEST_TABLE_1 = "testtable1";
  public static final String JOIN_TEST_TABLE_2 = "testtable2";

  public static final int MASTER_ID = 0;

  public static final int MASTER_PORT = 8001;

  public static final int[] WORKER_ID = { 1, 2 };

  public static final int[] WORKER_PORT = { 9001, 9002 };
  public static Process SERVER_PROCESS;
  public static final Process[] workerProcess = new Process[WORKER_ID.length];
  public static final Thread[] workerStdoutReader = new Thread[WORKER_ID.length];

  public static String workerTestBaseFolder;

  public static void createTable(final String dbFileAbsolutePath, final String tableName, final String sqlSchemaString)
      throws IOException, CatalogException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      File f = new File(dbFileAbsolutePath);

      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(f);
      sqliteConnection.open(true);

      /* Create the table if not exist */
      statement = sqliteConnection.prepare("create table if not exists " + tableName + "(" + sqlSchemaString + ");");

      statement.step();
      statement.reset();

      /* Clear table data in case it already exists */
      statement = sqliteConnection.prepare("delete from " + tableName);
      statement.step();
      statement.reset();

    } catch (SQLiteException e) {
      throw new CatalogException(e);
    } finally {
      if (statement != null) {
        statement.dispose();
      }
      if (sqliteConnection != null) {
        sqliteConnection.dispose();
      }
    }
  }

  public static void createTable(final int workerID, final String dbFilename, final String tableName,
      final String sqlSchemaString) throws IOException, CatalogException {
    createTable(getAbsoluteDBFile(workerID, dbFilename).getAbsolutePath(), tableName, sqlSchemaString);
  }

  public static File getAbsoluteDBFile(final int workerID, String dbFilename) throws CatalogException {
    if (!dbFilename.endsWith(".db")) {
      dbFilename = dbFilename + ".db";
    }
    // return new File(workerTestBaseFolder + File.separator + "worker_" + workerID + File.separator + dbFilename);
    String workerDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);
    WorkerCatalog wc = WorkerCatalog.open(FilenameUtils.concat(workerDir, "worker.catalog"));
    File ret = new File(FilenameUtils.concat(wc.getConfigurationValue("worker.data.sqlite.dir"), dbFilename));
    wc.close();
    return ret;
  }

  @AfterClass
  public static void globalCleanup() throws IOException {
    if (Server.runningInstance != null) {
      Server.runningInstance.cleanup();
    }

    for (Process p : workerProcess) {
      p.destroy();
    }

    for (Thread t : workerStdoutReader) {
      try {
        if (t != null) {
          t.join();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    FSUtils.blockingDeleteDirectory(workerTestBaseFolder);

    boolean finishClean = false;
    while (!finishClean) {
      finishClean = AvailablePortFinder.available(MASTER_PORT);
      for (final int workerPort : WORKER_PORT) {
        finishClean = finishClean && AvailablePortFinder.available(workerPort);
      }
      if (!finishClean) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }

    }
  }

  @BeforeClass
  public static void globalInit() throws IOException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    Path tempFilePath = Files.createTempDirectory(Server.SYSTEM_NAME + "_systemtests");
    workerTestBaseFolder = tempFilePath.toFile().getAbsolutePath();
    CatalogMaker.makeTwoNodeLocalParallelCatalog(workerTestBaseFolder);

    if (!AvailablePortFinder.available(MASTER_PORT)) {
      throw new RuntimeException("Unable to start master, port " + MASTER_PORT + " is taken");
    }
    for (int port : WORKER_PORT) {
      if (!AvailablePortFinder.available(port)) {
        throw new RuntimeException("Unable to start worker, port " + port + " is taken");
      }
    }

    startMaster();
    startWorkers();
  }

  public static void insert(final int workerID, final String tableName, final Schema schema, final TupleBatch data)
      throws CatalogException {
    String insertTemplate = SQLiteUtils.insertStatementFromSchema(schema, tableName);
    SQLiteAccessMethod.tupleBatchInsert(getAbsoluteDBFile(workerID, tableName).getAbsolutePath(), insertTemplate, data);
  }

  public static void insertWithBothNames(int workerID, String tableName, String dbName, Schema schema, TupleBatch data)
      throws CatalogException {
    String insertTemplate = SQLiteUtils.insertStatementFromSchema(schema, tableName);
    SQLiteAccessMethod.tupleBatchInsert(getAbsoluteDBFile(workerID, dbName).getAbsolutePath(), insertTemplate, data);
  }

  public static HashMap<Tuple, Integer> simpleRandomJoinTestBase() throws CatalogException, IOException {
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_1, JOIN_TEST_TABLE_1, "id long, name varchar(20)"); // worker 1 partition
                                                                                                  // of
    // table1
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_2, JOIN_TEST_TABLE_2, "id long, name varchar(20)"); // worker 1 partition
                                                                                                  // of
    // table2
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_1, JOIN_TEST_TABLE_1, "id long, name varchar(20)");// worker 2 partition
                                                                                                 // of
    // table1
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_2, JOIN_TEST_TABLE_2, "id long, name varchar(20)");// worker 2 partition
                                                                                                 // of
    // table2

    final String[] tbl1NamesWorker1 = TestUtils.randomFixedLengthNumericString(1000, 2000, 2, 20);
    final String[] tbl1NamesWorker2 = TestUtils.randomFixedLengthNumericString(1000, 2000, 2, 20);
    final long[] tbl1IDsWorker1 = TestUtils.randomLong(1000, 2000, 2);
    final long[] tbl1IDsWorker2 = TestUtils.randomLong(1000, 2000, 2);

    final String[] tbl2NamesWorker1 = TestUtils.randomFixedLengthNumericString(2001, 3000, 200, 20);
    final String[] tbl2NamesWorker2 = TestUtils.randomFixedLengthNumericString(2001, 3000, 200, 20);
    final long[] tbl2IDsWorker1 = TestUtils.randomLong(2001, 3000, 200);
    final long[] tbl2IDsWorker2 = TestUtils.randomLong(2001, 3000, 200);

    final long[] idsCommon = TestUtils.randomLong(1, 1, 20);
    final String[] namesCommon = TestUtils.randomFixedLengthNumericString(1, 1, 20, 20);

    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tbl2Worker1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tbl2Worker2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);

    for (int i = 0; i < tbl1NamesWorker1.length; i++) {
      tbl1Worker1.put(0, tbl1IDsWorker1[i]);
      tbl1Worker1.put(1, tbl1NamesWorker1[i]);
    }
    for (int i = 0; i < tbl1NamesWorker2.length; i++) {
      tbl1Worker2.put(0, tbl1IDsWorker2[i]);
      tbl1Worker2.put(1, tbl1NamesWorker2[i]);
    }
    for (int i = 0; i < tbl2NamesWorker1.length; i++) {
      tbl2Worker1.put(0, tbl2IDsWorker1[i]);
      tbl2Worker1.put(1, tbl2NamesWorker1[i]);
    }
    for (int i = 0; i < tbl2NamesWorker2.length; i++) {
      tbl2Worker2.put(0, tbl2IDsWorker2[i]);
      tbl2Worker2.put(1, tbl2NamesWorker2[i]);
    }

    for (int i = 0; i < idsCommon.length; i++) {
      tbl1Worker1.put(0, idsCommon[i]);
      tbl1Worker1.put(1, namesCommon[i]);
      tbl2Worker2.put(0, idsCommon[i]);
      tbl2Worker2.put(1, namesCommon[i]);
    }

    final TupleBatchBuffer table1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    final TupleBatchBuffer table2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table2.merge(tbl2Worker1);
    table2.merge(tbl2Worker2);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.naturalJoin(table1, table2, 0, 0);

    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(WORKER_ID[0], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(WORKER_ID[1], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker1.popAny()) != null) {
      insert(WORKER_ID[0], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker2.popAny()) != null) {
      insert(WORKER_ID[1], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }

    return expectedResult;

  }

  static Server startMaster() {
    new Thread() {
      @Override
      public void run() {
        try {
          String catalogFileName = FilenameUtils.concat(workerTestBaseFolder, "master.catalog");
          Server.main(new String[] { catalogFileName });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }.start();
    return Server.runningInstance;
  }

  /**
   * Start workers in separate processes.
   * */
  static void startWorkers() throws IOException {
    int workerCount = 0;
    for (int i = 0; i < WORKER_ID.length; i++) {
      int workerID = WORKER_ID[i];
      String workingDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);

      final String[] workerClasspath = EclipseClasspathReader.readEclipseClasspath(new File(".classpath"));
      final ProcessBuilder pb =
          new ProcessBuilder(
              "java",
              "-Djava.library.path=" + workerClasspath[1],
              "-Dorg.jboss.netty.debug",
              "-Xdebug",
              // Now eclipse is able to debug remotely the worker processes following the steps:
              // 1. Set a breakpoint at the beginning of a JUnit test method.
              // 2. start debug the JUnit test method. The test method should stop at the preset breakpoint.
              // But now, the worker processes are already started.
              // 3. Create an Eclipse remote debugger and set to attach to localhost 10001 for worker1 and localhost
              // 10002 for worker2
              // 4. Now, you are able to debug the worker processes. All the Java debugging methods are supported such
              // as breakpoints.
              "-Xrunjdwp:transport=dt_socket,address=" + (SystemTestBase.WORKER_PORT[i] + 1000) + ",server=y,suspend=n",
              "-classpath", workerClasspath[0], Worker.class.getCanonicalName(), "--workingDir", workingDir);

      pb.directory(new File(workingDir));
      pb.redirectErrorStream(true);
      pb.redirectOutput(Redirect.PIPE);

      final int wc = workerCount;

      workerStdoutReader[wc] = new Thread() {

        int myWorkerIdx;

        @Override
        public void run() {
          myWorkerIdx = wc;
          try {
            workerProcess[wc] = pb.start();
            writeProcessOutput(workerProcess[wc]);
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        void writeProcessOutput(Process process) throws Exception {

          InputStreamReader tempReader = new InputStreamReader(new BufferedInputStream(process.getInputStream()));
          BufferedReader reader = new BufferedReader(tempReader);
          try {
            while (true) {
              String line = reader.readLine();
              if (line == null) {
                break;
              }
              System.out.println("localhost:" + WORKER_PORT[myWorkerIdx] + "$ " + line);
            }
          } catch (IOException e) {
            // remote has shutdown. Not an exception.
          }
        }
      };

      workerStdoutReader[wc].start();

      ++workerCount;

      try {
        // sleep 100 milliseconds.
        // yield the CPU so that the worker processes can be
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

}
