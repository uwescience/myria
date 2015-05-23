package edu.washington.escience.myria.systemtest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetEncoding;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myria.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.Worker;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.FSUtils;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.SQLiteUtils;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class SystemTestBase {

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(final Description description) {
      LOGGER.warn("*********************************************");
      LOGGER.warn(String.format("Starting test: %s()...", description.getMethodName()));
      LOGGER.warn("*********************************************");
    };
  };

  /** Automatically fail system tests that take longer than this many milliseconds. */
  private final int SYSTEM_TEST_TIMEOUT_MILLIS = 120 * 1000;
  @Rule
  public TestRule globalTimeout = Timeout.millis(SYSTEM_TEST_TIMEOUT_MILLIS);
  @Rule
  public TestName name = new TestName();

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SystemTestBase.class);

  public static final Schema JOIN_INPUT_SCHEMA = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE),
      ImmutableList.of("id", "name"));
  public static final RelationKey JOIN_TEST_TABLE_1 = RelationKey.of("test", "test", "testtable1");
  public static final RelationKey JOIN_TEST_TABLE_2 = RelationKey.of("test", "test", "testtable2");

  public static final int MASTER_ID = 0;

  public static final int DEFAULT_MASTER_PORT_ = 8001;

  public static final int DEFAULT_REST_PORT = 8753;

  /** Wait up to 15 seconds per work for workers to start. */
  public int WORKER_BOOTUP_TIMEOUT_IN_SECOND_PER_WORKER = 15;

  public volatile int masterPort;
  public volatile int masterDaemonPort = DEFAULT_REST_PORT;

  public static final int DEFAULT_WORKER_STARTING_PORT = 9001;

  public static Process SERVER_PROCESS;

  public volatile int[] workerIDs;
  public volatile int[] workerPorts;
  public volatile Process[] workerProcess;
  public volatile Thread[] workerStdoutReader;

  public volatile static String workerTestBaseFolder;

  /** Whether to run the worker and master daemons in debug mode. */
  public static final boolean DEBUG = false;
  /** How much memory the system tests might use. */
  public static final String MEMORY = "512M";

  public static void createTable(final int workerID, final RelationKey relationKey, final String sqlSchemaString)
      throws IOException, CatalogException {
    try {
      SQLiteUtils.createTable(getAbsoluteDBFile(workerID).getAbsolutePath(), relationKey, sqlSchemaString, true, true);
    } catch (SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  public static void deleteTable(final int workerID, final RelationKey relationKey) throws IOException,
      CatalogException {
    try {
      SQLiteUtils.deleteTable(getAbsoluteDBFile(workerID).getAbsolutePath(), relationKey);
    } catch (SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  public static boolean existsTable(final int workerID, final RelationKey relationKey) throws IOException,
      CatalogException {
    try {
      return SQLiteUtils.existsTable(getAbsoluteDBFile(workerID).getAbsolutePath(), relationKey);
    } catch (SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  public static File getAbsoluteDBFile(final int workerID) throws CatalogException, FileNotFoundException {
    final String workerDir = getWorkerFolder(workerID);
    final WorkerCatalog wc = WorkerCatalog.open(FilenameUtils.concat(workerDir, "worker.catalog"));
    final SQLiteInfo sqliteInfo =
        (SQLiteInfo) ConnectionInfo.of(MyriaConstants.STORAGE_SYSTEM_SQLITE, wc
            .getConfigurationValue(MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_CONN_INFO));
    final File ret = new File(sqliteInfo.getDatabaseFilename());
    wc.close();
    return ret;
  }

  protected static String getContents(final HttpURLConnection conn) {
    /* If there was any content returned, get it. */
    String content = null;
    try {
      InputStream is = conn.getInputStream();
      if (is != null) {
        content = ByteString.readFrom(is).toStringUtf8();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    /* If there was any error returned, get it. */
    String error = null;
    try {
      InputStream is = conn.getErrorStream();
      if (is != null) {
        error = ByteString.readFrom(is).toStringUtf8();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    StringBuilder ret = new StringBuilder();
    if (content != null) {
      ret.append("Content:\n").append(content);
    }
    if (error != null) {
      ret.append("Error:\n").append(error);
    }
    return ret.toString();
  }

  /**
   * Override this if you want to run some code after each system test.
   */
  public void after() throws Exception {
  }

  @After
  public void globalCleanup() throws Exception {
    masterDaemon.stop();
    masterDaemon = null;
    server = null;

    for (final Thread t : workerStdoutReader) {
      try {
        if (t != null) {
          t.join();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
        return;
      }
    }

    FSUtils.blockingDeleteDirectory(workerTestBaseFolder);

    boolean finishClean = false;
    while (!finishClean) {
      finishClean = AvailablePortFinder.available(masterPort);
      finishClean = finishClean && AvailablePortFinder.available(masterDaemonPort);
      for (final int workerPort : workerPorts) {
        finishClean = finishClean && AvailablePortFinder.available(workerPort);
        if (DEBUG) {
          // make sure the JDWP listening ports are also successfully released.
          finishClean = finishClean && AvailablePortFinder.available(workerPort + 1000);
        }
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
      throw new IllegalStateException("did not finish clean!");
    } else {
      LOGGER.warn("Finish SystemTestBase cleanup.");
    }

    after();
  }

  public Map<String, String> getMasterConfigurations() {
    return Collections.<String, String> emptyMap();
  }

  public Map<String, String> getWorkerConfigurations() {
    return Collections.<String, String> emptyMap();
  }

  public Map<Integer, SocketInfo> getMasters() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    m.put(MyriaConstants.MASTER_ID, new SocketInfo(DEFAULT_MASTER_PORT_));
    return m;
  }

  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    Random r = new Random();
    m.put(MyriaConstants.MASTER_ID + r.nextInt(100) + 1, new SocketInfo(DEFAULT_WORKER_STARTING_PORT));
    m.put(MyriaConstants.MASTER_ID + r.nextInt(100) + 101, new SocketInfo(DEFAULT_WORKER_STARTING_PORT + 1));
    return m;
  }

  /**
   * Override this if you want to run some code before each system test.
   */
  public void before() throws Exception {
  }

  @Before
  public void globalInit() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final Path tempFilePath = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_systemtests");
    workerTestBaseFolder = tempFilePath.toFile().getAbsolutePath();
    Map<String, String> masterConfigs = getMasterConfigurations();
    Map<String, String> workerConfigs = getWorkerConfigurations();

    Map<Integer, SocketInfo> masters = getMasters();
    Map<Integer, SocketInfo> workers = getWorkers();

    CatalogMaker.makeNNodesLocalParallelCatalog(workerTestBaseFolder, masters, workers, masterConfigs, workerConfigs);

    for (Entry<Integer, SocketInfo> master : masters.entrySet()) {
      masterPort = master.getValue().getPort();
    }

    workerPorts = new int[workers.size()];
    workerIDs = new int[workerPorts.length];
    workerProcess = new Process[workerPorts.length];
    workerStdoutReader = new Thread[workerPorts.length];

    int i = 0;
    for (Entry<Integer, SocketInfo> worker : workers.entrySet()) {
      workerPorts[i] = worker.getValue().getPort();
      workerIDs[i] = worker.getKey();
      /** Make the worker folder. */
      Path workerPath = FileSystems.getDefault().getPath(getWorkerFolder(workerIDs[i]));
      Files.createDirectories(workerPath);
      i++;
    }

    if (!AvailablePortFinder.available(masterPort)) {
      throw new RuntimeException("Unable to start master, port " + masterPort + " is taken");
    }
    if (!AvailablePortFinder.available(masterDaemonPort)) {
      throw new RuntimeException("Unable to start master api server, port " + masterDaemonPort + " is taken");
    }
    for (final int port : workerPorts) {
      if (!AvailablePortFinder.available(port)) {
        throw new RuntimeException("Unable to start worker, port " + port + " is taken");
      }
    }

    startMaster();
    startWorkers();

    /* Wait until all the workers have connected to the master. */
    Set<Integer> targetWorkers = new HashSet<Integer>();
    for (int j : workerIDs) {
      targetWorkers.add(j);
    }

    long milliTimeout = TimeUnit.SECONDS.toMillis(WORKER_BOOTUP_TIMEOUT_IN_SECOND_PER_WORKER * workers.size());
    for (long start = System.currentTimeMillis(); !server.getAliveWorkers().containsAll(targetWorkers)
        && System.currentTimeMillis() - start < milliTimeout;) {
      Thread.sleep(500);
    }
    targetWorkers.removeAll(server.getAliveWorkers());
    if (!targetWorkers.isEmpty()) {
      throw new IllegalStateException("Workers: " + targetWorkers + " booting up timout");
    }

    // for setting breakpoint
    System.currentTimeMillis();

    before();
  }

  public static void insert(final int workerID, final RelationKey relationKey, final Schema schema,
      final TupleBatch data) throws CatalogException, FileNotFoundException, DbException {
    SQLiteAccessMethod
        .tupleBatchInsert(SQLiteInfo.of(getAbsoluteDBFile(workerID).getAbsolutePath()), relationKey, data);
  }

  protected HashMap<Tuple, Integer> simpleRandomJoinTestBase() throws CatalogException, IOException, DbException {
    /* worker 1 partition of table1 */
    createTable(workerIDs[0], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    /* worker 1 partition of table2 */
    createTable(workerIDs[0], JOIN_TEST_TABLE_2, "id long, name varchar(20)");
    /* worker 2 partition of table1 */
    createTable(workerIDs[1], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    /* worker 2 partition of table2 */
    createTable(workerIDs[1], JOIN_TEST_TABLE_2, "id long, name varchar(20)");

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
      tbl1Worker1.putLong(0, tbl1IDsWorker1[i]);
      tbl1Worker1.putString(1, tbl1NamesWorker1[i]);
    }
    for (int i = 0; i < tbl1NamesWorker2.length; i++) {
      tbl1Worker2.putLong(0, tbl1IDsWorker2[i]);
      tbl1Worker2.putString(1, tbl1NamesWorker2[i]);
    }
    for (int i = 0; i < tbl2NamesWorker1.length; i++) {
      tbl2Worker1.putLong(0, tbl2IDsWorker1[i]);
      tbl2Worker1.putString(1, tbl2NamesWorker1[i]);
    }
    for (int i = 0; i < tbl2NamesWorker2.length; i++) {
      tbl2Worker2.putLong(0, tbl2IDsWorker2[i]);
      tbl2Worker2.putString(1, tbl2NamesWorker2[i]);
    }

    for (int i = 0; i < idsCommon.length; i++) {
      tbl1Worker1.putLong(0, idsCommon[i]);
      tbl1Worker1.putString(1, namesCommon[i]);
      tbl2Worker2.putLong(0, idsCommon[i]);
      tbl2Worker2.putString(1, namesCommon[i]);
    }

    final TupleBatchBuffer table1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);

    final TupleBatchBuffer table2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table2.unionAll(tbl2Worker1);
    table2.unionAll(tbl2Worker2);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.naturalJoin(table1, table2, 0, 0);

    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }

    return expectedResult;

  }

  protected HashMap<Tuple, Integer> simpleFixedJoinTestBase() throws CatalogException, IOException, DbException {
    // worker 1 partition of table1
    createTable(workerIDs[0], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    // worker 1 partition of table2
    createTable(workerIDs[0], JOIN_TEST_TABLE_2, "id long, name varchar(20)");
    // worker 2 partition of table1
    createTable(workerIDs[1], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    // worker 2 partition of table2
    createTable(workerIDs[1], JOIN_TEST_TABLE_2, "id long, name varchar(20)");

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
      tbl1Worker1.putLong(0, tbl1IDsWorker1[i]);
      tbl1Worker1.putString(1, tbl1NamesWorker1[i]);
    }
    for (int i = 0; i < tbl1NamesWorker2.length; i++) {
      tbl1Worker2.putLong(0, tbl1IDsWorker2[i]);
      tbl1Worker2.putString(1, tbl1NamesWorker2[i]);
    }
    for (int i = 0; i < tbl2NamesWorker1.length; i++) {
      tbl2Worker1.putLong(0, tbl2IDsWorker1[i]);
      tbl2Worker1.putString(1, tbl2NamesWorker1[i]);
    }
    for (int i = 0; i < tbl2NamesWorker2.length; i++) {
      tbl2Worker2.putLong(0, tbl2IDsWorker2[i]);
      tbl2Worker2.putString(1, tbl2NamesWorker2[i]);
    }

    final TupleBatchBuffer table1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);

    final TupleBatchBuffer table2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table2.unionAll(tbl2Worker1);
    table2.unionAll(tbl2Worker2);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.naturalJoin(table1, table2, 0, 0);

    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tbl2Worker2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }

    return expectedResult;

  }

  /** The Server being run for the system test. */
  protected volatile static Server server;

  protected volatile static MasterDaemon masterDaemon;

  void startMaster() throws Exception {
    masterDaemon = new MasterDaemon(workerTestBaseFolder, masterDaemonPort);
    server = masterDaemon.getClusterMaster();
    server.start();
  }

  /**
   * @param workerId the id of the worker
   * @return the folder containing that worker's files.
   */
  public static String getWorkerFolder(final int workerId) {
    return FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerId);
  }

  /**
   * Start workers in separate processes.
   * */
  void startWorkers() throws IOException {
    int workerCount = 0;

    LOGGER.info("Workers for test [" + name.getMethodName() + "] are " + ArrayUtils.toString(workerIDs));
    for (int i = 0; i < workerIDs.length; i++) {
      final int workerID = workerIDs[i];
      final String workingDir = getWorkerFolder(workerID);

      String cp = System.getProperty("java.class.path");
      String lp = System.getProperty("java.library.path");

      /* Construct the arguments to start the new Java process. First, set up the JVM options. */
      ImmutableList.Builder<String> args = ImmutableList.builder();
      args.add("java") // run java
          .add("-ea") // enable assertions
          .add("-Djava.library.path=" + lp).add("-classpath").add(cp) // paths
          .add("-Xmx" + MEMORY) // memory limit to MEMORY
          .add("-XX:+HeapDumpOnOutOfMemoryError") //
          .add("-XX:HeapDumpPath=/tmp/worker_" + workerID + ".bin");

      /* If this test was run with a Java agent, then add it. */
      List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
      for (String s : inputArgs) {
        if (s.startsWith("-javaagent")) {
          String currentDirectory = new File(".").getCanonicalPath();
          String javaAgent = s.replaceAll("build/", currentDirectory + File.separator + "build/");
          args.add(javaAgent);
          System.err.println("Enabled java agent: " + javaAgent);
        }
      }

      /* Second, set up the JVM debug options. */
      if (DEBUG) {
        args.add("-Dorg.jboss.netty.debug").add("-Xdebug")
        // Now eclipse is able to debug remotely the worker processes
        // following the steps:
        // 1. Set a breakpoint at the beginning of a JUnit test method.
        // 2. start debug the JUnit test method. The test method should stop
        // at the preset breakpoint.
        // But now, the worker processes are already started.
        // 3. Create an Eclipse remote debugger and set to attach to localhost
        // 10001 for worker1 and localhost
        // 10002 for worker2
        // 4. Now, you are able to debug the worker processes. All the Java
        // debugging methods are supported such
        // as breakpoints.
            .add("-Xrunjdwp:transport=dt_socket,address=" + (workerPorts[i] + 1000) + ",server=y,suspend=n");
      }

      /* Finally, set up the class to be run (Worker) and its command-line options. */
      args.add(Worker.class.getCanonicalName(), "--workingDir", workingDir, "--testMethod", name.getMethodName());

      final ProcessBuilder pb = new ProcessBuilder(args.build());

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

              LOGGER.info("[" + name.getMethodName() + "]" + "#" + workerIDs[myWorkerIdx] + "@localhost:"
                  + workerPorts[myWorkerIdx] + "$ " + line);
            }
          } catch (final IOException e) {
            // remote has shutdown. Not an exception.
          }
        }
      };

      workerStdoutReader[wc].setName("WorkerStdoutReader-" + workerIDs[wc]);
      workerStdoutReader[wc].start();

      ++workerCount;
    }
  }

  public static QueryStatusEncoding getQueryStatus(final HttpURLConnection conn) throws IOException {
    ObjectReader reader = MyriaJsonMapperProvider.getReader().withType(QueryStatusEncoding.class);
    String s = IOUtils.toString(conn.getInputStream());
    try {
      return reader.readValue(s);
    } catch (IOException e) {
      throw new IOException("Error deserializing QueryStatusEncoding from " + s, e);
    }
  }

  public static DatasetStatus getDatasetStatus(final HttpURLConnection conn) throws IOException {
    ObjectReader reader = MyriaJsonMapperProvider.getReader().withType(DatasetStatus.class);
    return reader.readValue(conn.getInputStream());
  }

  protected HttpURLConnection submitQuery(final QueryEncoding query) throws IOException {
    ObjectWriter writer = MyriaJsonMapperProvider.getWriter();
    String queryString = writer.writeValueAsString(query);
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryString);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    return conn;
  }

  protected static String ingest(final RelationKey key, final Schema schema, final DataSource source,
      @Nullable final Character delimiter, @Nullable final PartitionFunction pf) throws JsonProcessingException {
    DatasetEncoding ingest = new DatasetEncoding();
    ingest.relationKey = key;
    ingest.schema = schema;
    ingest.source = source;
    if (delimiter != null) {
      ingest.delimiter = delimiter;
    }
    if (pf != null) {
      ingest.partitionFunction = pf;
    }
    return MyriaJsonMapperProvider.getWriter().writeValueAsString(ingest);
  }
}
