package edu.washington.escience.myriad.systemtest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
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

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      LOGGER.warn("*********************************************");
      LOGGER.warn(String.format("Starting test: %s()...", description.getMethodName()));
      LOGGER.warn("*********************************************");
    };
  };

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
      if (this == o) {
        return true;
      }
      if (!(o instanceof Tuple)) {
        return false;
      }
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
      final StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < values.length - 1; i++) {
        final Comparable<?> v = values[i];
        sb.append(v + ", ");
      }
      sb.append(values[values.length - 1] + ")");
      return sb.toString();
    }
  }

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SystemTestBase.class.getName());

  public static final Schema JOIN_INPUT_SCHEMA = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE),
      ImmutableList.of("id", "name"));
  public static final RelationKey JOIN_TEST_TABLE_1 = RelationKey.of("test", "test", "testtable1");
  public static final RelationKey JOIN_TEST_TABLE_2 = RelationKey.of("test", "test", "testtable2");

  public static final int MASTER_ID = 0;

  public static final int DEFAULT_MASTER_PORT_ = 8001;

  public volatile int masterPort;
  public volatile int[] workerPorts;

  public static final int[] WORKER_ID = { 1, 2 };

  // public static final int[] DEFAULT_WORKER_PORTS = { 9001, 9002 };
  public static final int DEFAULT_WORKER_STARTING_PORT = 9001;

  public static Process SERVER_PROCESS;
  public static final Process[] workerProcess = new Process[WORKER_ID.length];
  public static final Thread[] workerStdoutReader = new Thread[WORKER_ID.length];

  public static String workerTestBaseFolder;

  public static void createTable(final int workerID, final RelationKey relationKey, final String sqlSchemaString)
      throws IOException, CatalogException {
    createTable(getAbsoluteDBFile(workerID).getAbsolutePath(), relationKey, sqlSchemaString);
  }

  public static void createTable(final String dbFileAbsolutePath, final RelationKey relationKey,
      final String sqlSchemaString) throws IOException, CatalogException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      final File f = new File(dbFileAbsolutePath);

      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(f);
      sqliteConnection.open(true);

      /* Create the table if not exist */
      statement =
          sqliteConnection.prepare("create table if not exists "
              + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " (" + sqlSchemaString + ");");

      statement.step();
      statement.reset();

      /* Clear table data in case it already exists */
      statement = sqliteConnection.prepare("delete from " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
      statement.step();
      statement.reset();

    } catch (final SQLiteException e) {
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

  public static File getAbsoluteDBFile(final int workerID) throws CatalogException, FileNotFoundException {
    final String workerDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);
    final WorkerCatalog wc = WorkerCatalog.open(FilenameUtils.concat(workerDir, "worker.catalog"));
    final File ret = new File(wc.getConfigurationValue(MyriaSystemConfigKeys.WORKER_DATA_SQLITE_DB));
    wc.close();
    return ret;
  }

  @After
  public void globalCleanup() throws IOException {
    server.shutdown();
    server = null;
    Server.resetRunningInstance();

    for (final Thread t : workerStdoutReader) {
      try {
        if (t != null) {
          t.join();
        }
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    FSUtils.blockingDeleteDirectory(workerTestBaseFolder);

    boolean finishClean = false;
    while (!finishClean) {
      finishClean = AvailablePortFinder.available(masterPort);
      for (final int workerPort : workerPorts) {
        finishClean = finishClean && AvailablePortFinder.available(workerPort);
      }
      for (final int workerPort : workerPorts) {
        // make sure the JDWP listening ports are also successfully released.
        finishClean = finishClean && AvailablePortFinder.available(workerPort + 1000);
      }
      if (!finishClean) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    if (!finishClean) {
      LOGGER.warn("did not finish clean!");
    }
  }

  public Map<String, String> getMasterConfigurations() {
    HashMap<String, String> mc = new HashMap<String, String>();
    mc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, DEFAULT_MASTER_PORT_ + "");
    return mc;
  }

  public Map<String, String> getWorkerConfigurations() {
    HashMap<String, String> wc = new HashMap<String, String>();
    wc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, DEFAULT_WORKER_STARTING_PORT + "");
    return wc;
  }

  @Before
  public void globalInit() throws IOException, InterruptedException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final Path tempFilePath = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_systemtests");
    workerTestBaseFolder = tempFilePath.toFile().getAbsolutePath();
    Map<String, String> masterConfigs = getMasterConfigurations();
    Map<String, String> workerConfigs = getWorkerConfigurations();
    String masterPortStr = masterConfigs.get(MyriaSystemConfigKeys.IPC_SERVER_PORT);
    if (masterPortStr == null) {
      masterPortStr = DEFAULT_MASTER_PORT_ + "";
      masterConfigs.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, masterPortStr);
    }
    masterPort = Integer.valueOf(masterPortStr);
    workerPorts = new int[2];
    String workerStartingPortStr = workerConfigs.get(MyriaSystemConfigKeys.IPC_SERVER_PORT);
    if (workerStartingPortStr == null) {
      workerStartingPortStr = DEFAULT_WORKER_STARTING_PORT + "";
      workerConfigs.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, workerStartingPortStr);
    }
    workerPorts[0] = Integer.valueOf(workerStartingPortStr);
    workerPorts[1] = workerPorts[0] + 1;
    CatalogMaker.makeNNodesLocalParallelCatalog(workerTestBaseFolder, 2, masterConfigs, workerConfigs);

    if (!AvailablePortFinder.available(masterPort)) {
      throw new RuntimeException("Unable to start master, port " + masterPort + " is taken");
    }
    for (final int port : workerPorts) {
      if (!AvailablePortFinder.available(port)) {
        throw new RuntimeException("Unable to start worker, port " + port + " is taken");
      }
    }

    startMaster();
    startWorkers();
    // for setting breakpoint
    if (System.currentTimeMillis() < 0) {
      System.out.println("Only for setting breakpoint. Never reach here");
    }
  }

  public static void insert(final int workerID, final RelationKey relationKey, final Schema schema,
      final TupleBatch data) throws CatalogException, FileNotFoundException {
    final String insertTemplate = SQLiteUtils.insertStatementFromSchema(schema, relationKey);
    SQLiteAccessMethod.tupleBatchInsert(getAbsoluteDBFile(workerID).getAbsolutePath(), insertTemplate, data);
  }

  public static HashMap<Tuple, Integer> simpleRandomJoinTestBase() throws CatalogException, IOException {
    /* worker 1 partition of table1 */
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    /* worker 1 partition of table2 */
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_2, "id long, name varchar(20)");
    /* worker 2 partition of table1 */
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    /* worker 2 partition of table2 */
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_2, "id long, name varchar(20)");

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

  public static HashMap<Tuple, Integer> simpleFixedJoinTestBase() throws CatalogException, IOException {
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_1, "id long, name varchar(20)"); // worker 1 partition
                                                                               // of
    // table1
    createTable(WORKER_ID[0], JOIN_TEST_TABLE_2, "id long, name varchar(20)"); // worker 1 partition
                                                                               // of
    // table2
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_1, "id long, name varchar(20)");// worker 2 partition
                                                                              // of
    // table1
    createTable(WORKER_ID[1], JOIN_TEST_TABLE_2, "id long, name varchar(20)");// worker 2 partition
                                                                              // of
    // table2

    final String[] tbl1NamesWorker1 = new String[] { "tb1_111", "tb1_222", "tb1_333" };
    final String[] tbl1NamesWorker2 = new String[] { "tb1_444", "tb1_555", "tb1_666" };
    final long[] tbl1IDsWorker1 = new long[] { 111, 222, 333 };
    final long[] tbl1IDsWorker2 = new long[] { 444, 555, 666 };

    final String[] tbl2NamesWorker1 = new String[] { "tb2_444", "tb2_555", "tb2_666" };
    final String[] tbl2NamesWorker2 = new String[] { "tb2_111", "tb2_222", "tb2_333" };

    final long[] tbl2IDsWorker1 = new long[] { 444, 555, 666 };
    final long[] tbl2IDsWorker2 = new long[] { 111, 222, 333 };

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

  /** The Server being run for the system test. */
  protected volatile static Server server;

  static Server startMaster() throws InterruptedException {
    new Thread("Master main thread") {
      @Override
      public void run() {
        try {
          final String catalogFileName = FilenameUtils.concat(workerTestBaseFolder, "master.catalog");
          // server = new Server(catalogFileName);
          // server.start();
          Server.main(new String[] { catalogFileName });
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }.start();
    while (server == null) {
      // Wait the Server to finish starting.
      Thread.sleep(100);
      server = Server.getRunningInstance();
    }
    return server;
  }

  /**
   * Start workers in separate processes.
   * */
  void startWorkers() throws IOException {
    int workerCount = 0;

    for (int i = 0; i < WORKER_ID.length; i++) {
      final int workerID = WORKER_ID[i];
      final String workingDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);

      final String[] workerClasspath = EclipseClasspathReader.readEclipseClasspath(new File(".classpath"));
      final ProcessBuilder pb =
          new ProcessBuilder(
              "java",
              "-ea", // enable assertion
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
              "-Xrunjdwp:transport=dt_socket,address=" + (workerPorts[i] + 1000) + ",server=y,suspend=n", "-classpath",
              workerClasspath[0], Worker.class.getCanonicalName(), "--workingDir", workingDir);

      pb.directory(new File(workingDir));
      pb.redirectErrorStream(true);
      pb.redirectOutput(Redirect.PIPE);

      final int wc = workerCount;

      workerStdoutReader[wc] = new Thread("Worker stdout reader#" + wc) {

        int myWorkerIdx;

        @Override
        public void run() {
          myWorkerIdx = wc;
          try {
            workerProcess[wc] = pb.start();
            writeProcessOutput(workerProcess[wc]);
          } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        void writeProcessOutput(final Process process) throws Exception {

          final InputStreamReader tempReader = new InputStreamReader(new BufferedInputStream(process.getInputStream()));
          final BufferedReader reader = new BufferedReader(tempReader);
          try {
            while (true) {
              final String line = reader.readLine();
              if (line == null) {
                break;
              }
              LOGGER.info("localhost:" + workerPorts[myWorkerIdx] + "$ " + line);
            }
          } catch (final IOException e) {
            // remote has shutdown. Not an exception.
          }
        }
      };

      workerStdoutReader[wc].setName("WorkerStdoutReader-" + wc);
      workerStdoutReader[wc].start();

      ++workerCount;
    }
  }
}
