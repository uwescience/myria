package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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

  // public static final String workerTestBaseFolder = System.getProperty("java.io.tmpdir") + File.separator
  // + Server.SYSTEM_NAME + "_systemtests";

  public static void assertTupleBagEqual(HashMap<Tuple, Integer> expectedResult, HashMap<Tuple, Integer> actualResult) {
    Assert.assertEquals(expectedResult.size(), actualResult.size());
    for (Entry<Tuple, Integer> e : actualResult.entrySet()) {
      assertTrue(expectedResult.get(e.getKey()).equals(e.getValue()));
    }
  }

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

  public static HashMap<Tuple, Integer> distinct(final TupleBatchBuffer content) {
    final Iterator<TupleBatch> it = content.getAll().iterator();
    final HashMap<Tuple, Integer> expectedResults = new HashMap<Tuple, Integer>();
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
        expectedResults.put(t, 1);
      }
    }
    return expectedResults;

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

    fullyDeleteDirectory(workerTestBaseFolder);

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

    // <<<<<<< HEAD
    // /* Create the test folder, e.g., in $tmp/Myriad_systemtests. */
    // final File testBaseFolderF = new File(workerTestBaseFolder);
    // testBaseFolderF.mkdirs();
    //
    // /* Create one folder for each Worker. */
    // for (final int workerID : WORKER_ID) {
    // final File f = new File(workerTestBaseFolder + File.separator + "worker_" + workerID);
    // while (!f.exists()) {
    // f.mkdirs();
    // }
    // }
    // =======
    Path tempFilePath = Files.createTempDirectory(Server.SYSTEM_NAME + "_systemtests");
    workerTestBaseFolder = tempFilePath.toFile().getAbsolutePath();
    CatalogMaker.makeTwoNodeLocalParallelCatalog(workerTestBaseFolder);
    // >>>>>>> master

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
          tuples.put(t, occur + 1);
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

  public static String[] readEclipseClasspath(final File eclipseClasspathXMLFile) throws SAXException, IOException,
      ParserConfigurationException {

    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

    final Document doc = dBuilder.parse(eclipseClasspathXMLFile);
    doc.getDocumentElement().normalize();

    final NodeList nList = doc.getElementsByTagName("classpathentry");

    final String separator = System.getProperty("path.separator");
    final StringBuilder classpathSB = new StringBuilder();
    for (int i = 0; i < nList.getLength(); i++) {
      final Node node = nList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        final Element e = (Element) node;

        final String kind = e.getAttribute("kind");
        if (kind.equals("output")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
        if (kind.equals("lib")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
      }
    }
    final NodeList attributeList = doc.getElementsByTagName("attribute");
    final StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      final Node node = attributeList.item(i);
      String value = null;
      if (node.getNodeType() == Node.ELEMENT_NODE
          && ("org.eclipse.jdt.launching.CLASSPATH_ATTR_LIBRARY_PATH_ENTRY".equals(((Element) node)
              .getAttribute("name"))) && ((value = ((Element) node).getAttribute("value")) != null)) {
        File f = new File(value);
        while (value != null && value.length() > 0 && !f.exists()) {
          value = value.substring(value.indexOf(File.separator) + 1);
          f = new File(value);
        }
        if (f.exists()) {
          libPathSB.append(f.getAbsolutePath() + separator);
        }
      }
    }
    return new String[] { classpathSB.toString(), libPathSB.toString() };
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
    try {
      int workerCount = 0;
      for (final int workerID : WORKER_ID) {

        // final File siteXMLF = new File(workingDIR + File.separator + "site.xml");
        // final File serverCONF = new File(workingDIR + File.separator + "server.conf");
        // final File workerCONF = new File(workingDIR + File.separator + "workers.conf");

        // final Configuration conf = new Configuration();
        // conf.set("worker.identifier", "" + workerID);
        // conf.set("worker.data.sqlite.dir", workingDIR);
        // conf.set("worker.tmp.dir", workingDIR);

        // siteXMLF.createNewFile();
        // conf.writeXml(new FileOutputStream(siteXMLF));
        //
        // FileOutputStream fos = new FileOutputStream(serverCONF);
        // fos.write(("localhost:" + MASTER_PORT).getBytes());
        // fos.close();

        // fos = new FileOutputStream(workerCONF);
        // for (final int workerPort : WORKER_PORT) {
        // fos.write(("localhost:" + workerPort + "\n").getBytes());
        // }
        // fos.close();
        String workingDir = FilenameUtils.concat(workerTestBaseFolder, "worker_" + workerID);

        final String[] workerClasspath = readEclipseClasspath(new File(".classpath"));
        final ProcessBuilder pb =
            new ProcessBuilder("java", "-Djava.library.path=" + workerClasspath[1], "-classpath", workerClasspath[0],
                Worker.class.getCanonicalName(), "--workingDir", workingDir);

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
            while (true) {
              String line = reader.readLine();
              if (line == null) {
                break;
              }
              System.out.println("localhost:" + WORKER_PORT[myWorkerIdx] + "$ " + line);
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
    } catch (final SAXException e) {
      throw new IOException(e);
    } catch (final ParserConfigurationException e) {
      throw new IOException(e);
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

  public static HashMap<Tuple, Integer> tupleBatchToTupleSet(final TupleBatchBuffer tbb) {
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
        result.put(t, 1);
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
