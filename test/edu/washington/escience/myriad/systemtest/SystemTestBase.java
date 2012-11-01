package edu.washington.escience.myriad.systemtest;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myriad.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.SQLiteTupleBatch;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.Worker;
import edu.washington.escience.myriad.table._TupleBatch;

public class SystemTestBase {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  static class Tuple implements Comparable<Tuple> {
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
  public static final Process[] WORKER_PROCESSES = new Process[WORKER_ID.length];
  public static String workerTestBaseFolder;

  public static void createTable(final int workerID, final String dbFilename, final String tableName,
      final String sqlSchemaString) throws CatalogException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(getAbsoluteDBFile(workerID, dbFilename));
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

  public static HashSet<Tuple> distinct(final TupleBatchBuffer content) {
    final Iterator<TupleBatch> it = content.getAll().iterator();
    final HashSet<Tuple> expectedResults = new HashSet<Tuple>();
    while (it.hasNext()) {
      final TupleBatch tb = it.next();
      final List<Column> columns = tb.outputRawData();
      final int numRow = columns.get(0).size();
      final int numColumn = columns.size();

      for (int i = 0; i < numRow; i++) {
        final Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, (Comparable<?>) columns.get(j).get(i));
        }
        expectedResults.add(t);
      }
    }
    return expectedResults;

  }

  public static File getAbsoluteDBFile(final int workerID, String dbFilename) throws CatalogException {
    if (!dbFilename.endsWith(".db")) {
      dbFilename = dbFilename + ".db";
    }
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

    fullyDeleteDirectory(workerTestBaseFolder);

    for (Process p : WORKER_PROCESSES) {
      p.destroy();
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

    startWorkers();
    startMaster();
  }

  public static void insert(final int workerID, final String tableName, final Schema schema, final _TupleBatch data)
      throws CatalogException {
    SQLiteTupleBatch
        .insertIntoSQLite(schema, tableName, getAbsoluteDBFile(workerID, tableName).getAbsolutePath(), data);
  }

  public static void insertWithBothNames(int workerID, String tableName, String dbName, Schema schema, _TupleBatch data)
      throws CatalogException {
    SQLiteTupleBatch.insertIntoSQLite(schema, tableName, getAbsoluteDBFile(workerID, dbName).getAbsolutePath(), data);
  }

  public static String intToString(final long v, final int length) {
    final StringBuilder sb = new StringBuilder("" + v);
    while (sb.length() < length) {
      sb.insert(0, "0");
    }
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  public static HashMap<Tuple, Integer> naturalJoin(final TupleBatchBuffer child1, final TupleBatchBuffer child2,
      final int child1JoinColumn, final int child2JoinColumn) {

    _TupleBatch tb = null;

    final HashMap<Comparable, HashMap<Tuple, Integer>> child1Hash = new HashMap<Comparable, HashMap<Tuple, Integer>>();

    int numChild1Column = 0;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    Iterator<TupleBatch> it = child1.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final List<Column> output = tb.outputRawData();
      final int numRow = output.get(0).size();
      final int numColumn = output.size();
      numChild1Column = numColumn;

      for (int i = 0; i < numRow; i++) {
        final Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, (Comparable<?>) output.get(j).get(i));
        }
        final Object v = t.get(child1JoinColumn);
        HashMap<Tuple, Integer> tuples = child1Hash.get(v);
        if (tuples == null) {
          tuples = new HashMap<Tuple, Integer>();
          tuples.put(t, 1);
          child1Hash.put((Comparable<?>) v, tuples);
        } else {
          final Integer occur = tuples.get(t);
          // if (occur == null) {
          // tuples.put(t, 1);
          // } else {
          tuples.put(t, occur + 1);
          // }
        }
      }
    }

    it = child2.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final List<Column> child2Columns = tb.outputRawData();
      final int numRow = child2Columns.get(0).size();
      final int numChild2Column = child2Columns.size();
      for (int i = 0; i < numRow; i++) {
        final Object v = child2Columns.get(child2JoinColumn).get(i);
        final HashMap<Tuple, Integer> matchedTuples = child1Hash.get(v);
        if (matchedTuples != null) {
          final Tuple child2Tuple = new Tuple(numChild2Column);

          for (int j = 0; j < numChild2Column; j++) {
            child2Tuple.set(j, (Comparable<?>) child2Columns.get(j).get(i));
          }

          for (final Entry<Tuple, Integer> entry : matchedTuples.entrySet()) {
            final Tuple child1Tuple = entry.getKey();
            final int numChild1Occur = entry.getValue();

            final Tuple t = new Tuple(numChild1Column + numChild2Column);
            t.setAll(0, child1Tuple);
            t.setAll(numChild1Column, child2Tuple);
            final Integer occur = result.get(t);
            if (occur == null) {
              result.put(t, numChild1Occur);
            } else {
              result.put(t, occur + numChild1Occur);
            }
          }
        }
      }
    }
    return result;

  }

  /***/
  public static String[] randomFixedLengthNumericString(final int min, final int max, final int size, final int length) {

    final String[] result = new String[size];
    final long[] intV = randomLong(min, max, size);

    for (int i = 0; i < size; i++) {
      result[i] = intToString(intV[i], length);
    }
    return result;
  }

  public static long[] randomLong(final long min, final long max, final int size) {
    final long[] result = new long[size];
    final Random r = new Random();
    final long top = max - min + 1;
    for (int i = 0; i < size; i++) {
      result[i] = r.nextInt((int) top) + min;
    }
    return result;
  }

  public static HashMap<Tuple, Integer> simpleRandomJoinTestBase() throws CatalogException {
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

    final String[] tbl1NamesWorker1 = randomFixedLengthNumericString(1000, 2000, 2, 20);
    final String[] tbl1NamesWorker2 = randomFixedLengthNumericString(1000, 2000, 2, 20);
    final long[] tbl1IDsWorker1 = randomLong(1000, 2000, 2);
    final long[] tbl1IDsWorker2 = randomLong(1000, 2000, 2);

    final String[] tbl2NamesWorker1 = randomFixedLengthNumericString(2001, 3000, 200, 20);
    final String[] tbl2NamesWorker2 = randomFixedLengthNumericString(2001, 3000, 200, 20);
    final long[] tbl2IDsWorker1 = randomLong(2001, 3000, 200);
    final long[] tbl2IDsWorker2 = randomLong(2001, 3000, 200);

    final long[] idsCommon = randomLong(1, 1, 20);
    final String[] namesCommon = randomFixedLengthNumericString(1, 1, 20, 20);

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

    final HashMap<Tuple, Integer> expectedResult = naturalJoin(table1, table2, 0, 0);

    _TupleBatch tb = null;
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
    for (final int workerID : WORKER_ID) {

      String workingDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);

      final String[] workerClasspath = ParallelUtility.readEclipseClasspath(new File(".classpath"));
      final ProcessBuilder pb =
          new ProcessBuilder("java", "-Djava.library.path=" + workerClasspath[1], "-classpath", workerClasspath[0],
              Worker.class.getCanonicalName(), "--workingDir", workingDir);

      pb.directory(new File(workingDir));
      pb.redirectErrorStream(true);
      pb.redirectOutput(Redirect.INHERIT);

      WORKER_PROCESSES[workerCount] = pb.start();
      ++workerCount;

      try {
        // sleep 100 milliseconds.
        // yield the CPU so that the worker processes can be
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
      // InputStream is = p.getInputStream();
      // BufferedReader br = new BufferedReader(new InputStreamReader(is));
      // String line = null;
      // while ((line = br.readLine()) != null) {
      // System.out.println(line);
      // }
    }
  }

  public static HashMap<Tuple, Integer> tupleBatchToTupleBag(final TupleBatchBuffer tbb) {
    _TupleBatch tb = null;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final int numRow = tb.numOutputTuples();
      final List<Column> columns = tb.outputRawData();
      final int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        final Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, (Comparable<?>) columns.get(column).get(row));
        }
        final Integer numOccur = result.get(t);
        if (numOccur == null) {
          result.put(t, new Integer(1));
        } else {
          result.put(t, numOccur + 1);
        }
      }
    }
    return result;
  }

  public static HashSet<Tuple> tupleBatchToTupleSet(final TupleBatchBuffer tbb) {
    _TupleBatch tb = null;
    final HashSet<Tuple> result = new HashSet<Tuple>();
    final Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final int numRow = tb.numOutputTuples();
      final List<Column> columns = tb.outputRawData();
      final int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        final Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, (Comparable<?>) columns.get(column).get(row));
        }
        result.add(t);
      }
    }
    return result;
  }

  public static void fullyDeleteDirectory(String pathToDirectory) {
    final File testBaseFolderF = new File(pathToDirectory);
    try {
      ParallelUtility.deleteFileFolder(testBaseFolderF);
    } catch (final IOException e) {
      e.printStackTrace();
    }
    boolean finishClean = false;
    while (!finishClean) {
      finishClean = !testBaseFolderF.exists();
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
  //
  // @After
  // public void cleanup() {
  // fullyDeleteDirectory(workerTestBaseFolder);
  // }
  //
  // @Before
  // public void init() {
  // CatalogMaker.makeTwoNodeLocalParallelCatalog(workerTestBaseFolder);
  // }
}
