package edu.washington.escience.myriad.systemtest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.mina.util.AvailablePortFinder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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
import edu.washington.escience.myriad.parallel.Configuration;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.SQLiteTupleBatch;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.Worker;
import edu.washington.escience.myriad.table._TupleBatch;

public class SystemTestBase {

  public static final int MASTER_ID = 0;
  public static final int[] WORKER_ID = { 1, 2 };
  public static final int[] WORKER_PORT = { 9001, 9002 };
  public static final int MASTER_PORT = 8001;

  public static final String workerTestBaseFolder = "/tmp/" + Server.SYSTEM_NAME + "_systemtests";

  static class Tuple implements Comparable<Tuple> {
    Comparable[] values;

    public Tuple(int numFields) {
      values = new Comparable[numFields];
    }

    public int numFields() {
      return values.length;
    }

    public Object get(int i) {
      return values[i];
    }

    public void set(int i, Comparable v) {
      values[i] = v;
    }

    public void setAll(int start, Tuple other) {
      System.arraycopy(other.values, 0, values, start, other.values.length);
    }

    @Override
    public int compareTo(Tuple o) {
      if (o == null || o.values.length != values.length) {
        throw new IllegalArgumentException("invalid tuple");
      }
      for (int i = 0; i < values.length; i++) {
        int sub = values[i].compareTo(o.values[i]);
        if (sub != 0) {
          return sub;
        }
      }
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      return compareTo((Tuple) o) == 0;
    }

    @Override
    public int hashCode() {
      int h = 1;
      for (Comparable o : values) {
        h = 31 * h + o.hashCode();
      }
      return h;
    }
  }

  @BeforeClass
  public static void globalInit() throws IOException {
    /*************** create test folder ******************/
    File testBaseFolderF = new File(workerTestBaseFolder);
    testBaseFolderF.mkdirs();

    for (int workerID : WORKER_ID) {
      File f = new File(workerTestBaseFolder + "/worker_" + workerID);
      while (!f.exists()) {
        f.mkdirs();
      }
    }

    startWorkers();
    startMaster();
  }

  public static final String JOIN_TEST_TABLE_1 = "testtable1";
  public static final String JOIN_TEST_TABLE_2 = "testtable2";
  public static final Schema JOIN_INPUT_SCHEMA = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE },
      new String[] { "id", "name" });

  public static HashMap<Tuple, Integer> simpleRandomJoinTestBase() throws IOException {
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

    String[] tbl1NamesWorker1 = randomFixedLengthNumericString(1000, 2000, 2, 20);
    String[] tbl1NamesWorker2 = randomFixedLengthNumericString(1000, 2000, 2, 20);
    long[] tbl1IDsWorker1 = randomLong(1000, 2000, 2);
    long[] tbl1IDsWorker2 = randomLong(1000, 2000, 2);

    String[] tbl2NamesWorker1 = randomFixedLengthNumericString(2001, 3000, 200, 20);
    String[] tbl2NamesWorker2 = randomFixedLengthNumericString(2001, 3000, 200, 20);
    long[] tbl2IDsWorker1 = randomLong(2001, 3000, 200);
    long[] tbl2IDsWorker2 = randomLong(2001, 3000, 200);

    long[] idsCommon = randomLong(1, 1, 20);
    String[] namesCommon = randomFixedLengthNumericString(1, 1, 20, 20);

    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    TupleBatchBuffer tbl2Worker1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    TupleBatchBuffer tbl2Worker2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);

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

    TupleBatchBuffer table1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    TupleBatchBuffer table2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table2.merge(tbl2Worker1);
    table2.merge(tbl2Worker2);

    HashMap<Tuple, Integer> expectedResult = naturalJoin(table1, table2, 0, 0);

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

  @Before
  public void init() throws IOException {
    File testBaseFolderF = new File(workerTestBaseFolder);
    testBaseFolderF.mkdirs();

    for (int workerID : WORKER_ID) {
      File f = new File(workerTestBaseFolder + "/worker_" + workerID);
      while (!f.exists()) {
        f.mkdirs();
      }
    }

  }

  public static HashSet<Tuple> tupleBatchToTupleSet(TupleBatchBuffer tbb) {
    _TupleBatch tb = null;
    HashSet<Tuple> result = new HashSet<Tuple>();
    Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      int numRow = tb.numOutputTuples();
      List<Column> columns = tb.outputRawData();
      int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, (Comparable) columns.get(column).get(row));
        }
        result.add(t);
      }
    }
    return result;
  }

  public static HashMap<Tuple, Integer> tupleBatchToTupleBag(TupleBatchBuffer tbb) {
    _TupleBatch tb = null;
    HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      int numRow = tb.numOutputTuples();
      List<Column> columns = tb.outputRawData();
      int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, (Comparable) columns.get(column).get(row));
        }
        Integer numOccur = result.get(t);
        if (numOccur == null) {
          result.put(t, new Integer(1));
        } else {
          result.put(t, numOccur + 1);
        }
      }
    }
    return result;
  }

  public static File getAbsoluteDBFile(int workerID, String dbFilename) {
    if (!dbFilename.endsWith(".db")) {
      dbFilename = dbFilename + ".db";
    }
    return new File(workerTestBaseFolder + "/worker_" + workerID + "/" + dbFilename);
  }

  public static void createTable(int workerID, String dbFilename, String tableName, String sqlSchemaString)
      throws IOException {
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

    } catch (final SQLiteException e) {
      e.printStackTrace();
      throw new IOException(e);
    } finally {
      statement.dispose();
      sqliteConnection.dispose();
    }
  }

  public static void insert(int workerID, String tableName, Schema schema, _TupleBatch data) {
    SQLiteTupleBatch
        .insertIntoSQLite(schema, tableName, getAbsoluteDBFile(workerID, tableName).getAbsolutePath(), data);
  }

  public static HashSet<Tuple> distinct(TupleBatchBuffer content) {
    Iterator<TupleBatch> it = content.getAll().iterator();
    HashSet<Tuple> expectedResults = new HashSet<Tuple>();
    while (it.hasNext()) {
      TupleBatch tb = it.next();
      List<Column> columns = tb.outputRawData();
      int numRow = columns.get(0).size();
      int numColumn = columns.size();

      for (int i = 0; i < numRow; i++) {
        Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, (Comparable) columns.get(j).get(i));
        }
        expectedResults.add(t);
      }
    }
    return expectedResults;

  }

  public static HashMap<Tuple, Integer> naturalJoin(TupleBatchBuffer child1, TupleBatchBuffer child2,
      int child1JoinColumn, int child2JoinColumn) {

    _TupleBatch tb = null;

    HashMap<Comparable, HashMap<Tuple, Integer>> child1Hash = new HashMap<Comparable, HashMap<Tuple, Integer>>();

    int numChild1Column = 0;
    HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    Iterator<TupleBatch> it = child1.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      List<Column> output = tb.outputRawData();
      int numRow = output.get(0).size();
      int numColumn = output.size();
      numChild1Column = numColumn;

      for (int i = 0; i < numRow; i++) {
        Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, (Comparable) output.get(j).get(i));
        }
        Object v = t.get(child1JoinColumn);
        HashMap<Tuple, Integer> tuples = child1Hash.get(v);
        if (tuples == null) {
          tuples = new HashMap<Tuple, Integer>();
          tuples.put(t, 1);
          child1Hash.put((Comparable) v, tuples);
        } else {
          Integer occur = tuples.get(t);
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
      List<Column> child2Columns = tb.outputRawData();
      int numRow = child2Columns.get(0).size();
      int numChild2Column = child2Columns.size();
      for (int i = 0; i < numRow; i++) {
        Object v = child2Columns.get(child2JoinColumn).get(i);
        HashMap<Tuple, Integer> matchedTuples = child1Hash.get(v);
        if (matchedTuples != null) {
          Tuple child2Tuple = new Tuple(numChild2Column);

          for (int j = 0; j < numChild2Column; j++) {
            child2Tuple.set(j, (Comparable) child2Columns.get(j).get(i));
          }

          for (Entry<Tuple, Integer> entry : matchedTuples.entrySet()) {
            Tuple child1Tuple = entry.getKey();
            int numChild1Occur = entry.getValue();

            Tuple t = new Tuple(numChild1Column + numChild2Column);
            t.setAll(0, child1Tuple);
            t.setAll(numChild1Column, child2Tuple);
            Integer occur = result.get(t);
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

  public static long[] randomLong(long min, long max, int size) {
    long[] result = new long[size];
    Random r = new Random();
    long top = max - min + 1;
    for (int i = 0; i < size; i++) {
      result[i] = r.nextInt((int) top) + min;
    }
    return result;
  }

  public static String intToString(long v, int length) {
    StringBuilder sb = new StringBuilder("" + v);
    while (sb.length() < length) {
      sb.insert(0, "0");
    }
    return sb.toString();
  }

  /***/
  public static String[] randomFixedLengthNumericString(int min, int max, int size, int length) {

    String[] result = new String[size];
    long[] intV = randomLong(min, max, size);

    for (int i = 0; i < size; i++) {
      result[i] = intToString(intV[i], length);
    }
    return result;
  }

  @After
  public void cleanup() {
    /*************** cleanup everything under the test folder **********/
    File testBaseFolderF = new File(workerTestBaseFolder);
    try {
      ParallelUtility.deleteFileFolder(testBaseFolderF);
    } catch (IOException e) {
      e.printStackTrace();
    }
    boolean finishClean = false;
    while (!finishClean) {
      finishClean = !testBaseFolderF.exists();
      if (!finishClean) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }

    }

  }

  @AfterClass
  public static void globalCleanup() {
    File testBaseFolderF = new File(workerTestBaseFolder);
    try {
      ParallelUtility.deleteFileFolder(testBaseFolderF);
    } catch (IOException e) {
      e.printStackTrace();
    }

    Server.runningInstance.cleanup();
    boolean finishClean = false;
    while (!finishClean) {
      finishClean = !testBaseFolderF.exists() && AvailablePortFinder.available(MASTER_PORT);
      for (int workerPort : WORKER_PORT) {
        finishClean = finishClean && AvailablePortFinder.available(workerPort);
      }
      if (!finishClean) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }

    }

  }

  public static String[] readEclipseClasspath(File eclipseClasspathXMLFile) throws SAXException, IOException,
      ParserConfigurationException {

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

    Document doc = dBuilder.parse(eclipseClasspathXMLFile);
    doc.getDocumentElement().normalize();

    NodeList nList = doc.getElementsByTagName("classpathentry");

    String separator = System.getProperty("path.separator");
    StringBuilder classpathSB = new StringBuilder();
    for (int i = 0; i < nList.getLength(); i++) {
      Node node = nList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element e = (Element) node;

        String kind = e.getAttribute("kind");
        if (kind.equals("output")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
        if (kind.equals("lib")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
      }
    }
    NodeList attributeList = doc.getElementsByTagName("attribute");
    StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      Node node = attributeList.item(i);
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

  static Server startMaster() throws IOException {
    new Thread() {
      @Override
      public void run() {
        try {
          Server.main(new String[] {});
        } catch (final Exception e) {
          e.printStackTrace();
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
      for (int workerID : WORKER_ID) {

        String workingDIR = workerTestBaseFolder + "/worker_" + workerID;
        File siteXMLF = new File(workingDIR + "/site.xml");
        File serverCONF = new File(workingDIR + "/server.conf");
        File workerCONF = new File(workingDIR + "/workers.conf");

        Configuration conf = new Configuration();
        conf.set("worker.identifier", "" + workerID);
        conf.set("worker.data.sqlite.dir", workingDIR);
        conf.set("worker.tmp.dir", workingDIR);

        siteXMLF.createNewFile();
        conf.writeXml(new FileOutputStream(siteXMLF));

        FileOutputStream fos = new FileOutputStream(serverCONF);
        fos.write(("localhost:" + MASTER_PORT).getBytes());
        fos.close();

        fos = new FileOutputStream(workerCONF);
        for (int workerPort : WORKER_PORT) {
          fos.write(("localhost:" + workerPort + "\n").getBytes());
        }
        fos.close();

        String[] workerClasspath = readEclipseClasspath(new File(".classpath"));
        System.out.println(workerClasspath);
        ProcessBuilder pb =
            new ProcessBuilder("java", "-Djava.library.path=" + workerClasspath[1], "-classpath", workerClasspath[0],
                Worker.class.getCanonicalName(), "--conf", workingDIR);

        pb.directory(new File(workingDIR));
        pb.redirectErrorStream(true);
        pb.redirectOutput(Redirect.INHERIT);

        Process p = pb.start();

        try {
          // sleep 100 milliseconds.
          // yield the CPU so that the worker processes can be
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
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
    } catch (SAXException e) {
      throw new IOException(e);
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
  }
}
