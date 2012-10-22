package edu.washington.escience.myriad.systemtest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.parallel.Configuration;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.Worker;

public class SystemTestBase {

  public static final int MASTER_ID = 0;
  public static final int[] WORKER_ID = { 1, 2 };
  public static final int[] WORKER_PORT = { 9001, 9002 };
  public static final int MASTER_PORT = 8001;

  public static final String[] sqliteSchema = new String[] { "id int,name char[20]", "id int,name char[20]" };

  public static final String[] inputTableName = new String[] { "testtable1", "testtable2" };

  public static final String[] inputDatabaseFileName = new String[] { "testdb1.db", "testdb2.db" };

  public static final String workerTestBaseFolder = "/tmp/" + Server.SYSTEM_NAME + "_systemtests";

  @Before
  public void init() throws IOException {

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

  public void createTable(int workerID, String tableName, String sqlSchemaString) throws IOException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      /* Connect to the database */
      sqliteConnection =
          new SQLiteConnection(new File(workerTestBaseFolder + "/worker_" + workerID + "/" + tableName + ".db"));
      sqliteConnection.open(true);

      /* Set up and execute the query */
      statement = sqliteConnection.prepare("create table if not exists " + tableName + "(" + sqlSchemaString + ");");

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

  @After
  public void cleanup() {
    /*************** cleanup everything under the test folder **********/
    File testBaseFolderF = new File(workerTestBaseFolder);
    try {
      ParallelUtility.deleteFileFolder(testBaseFolderF);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String readEclipseClasspath(File eclipseClasspathXMLFile) throws SAXException, IOException,
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
    return classpathSB.toString();
  }

  Server startMaster() throws IOException {
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
  void startWorkers() throws IOException {
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

        String workerClasspath = readEclipseClasspath(new File(".classpath"));
        System.out.println(workerClasspath);
        ProcessBuilder pb =
            new ProcessBuilder("java", "-classpath", workerClasspath, Worker.class.getCanonicalName(), "--conf",
                workingDIR);
        pb.directory(new File(workingDIR));
        pb.redirectErrorStream(true);
        pb.redirectOutput(Redirect.INHERIT);

        Process p = pb.start();

        try {
          // sleep 100 milliseconds. For 1:
          // yield the CPU so that
          Thread.sleep(100);
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
