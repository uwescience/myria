/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * @author valmeida
 * 
 */
public class BenchmarkTest {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BenchmarkTest.class);

  /**
   * The path to the dataset on your local machine. If not present, copy them from /projects/db7/passwords/jdbc_password
   * .
   */
  private final static String JDBC_PASSWORD_PATH = "data_nocommit/passwords/jdbc_password.txt";

  /** The JDBC password */
  private static String jdbcPassword = null;

  /** Whether we were able to copy the data. */
  private static boolean successfulSetup = false;

  /**
   * Test name.
   */
  private static final String BENCHMARKTEST_NAME = "dbmsBenchmark";

  /**
   * The hostname for the tests. betelgeuse has all databases installed and running. Please check that property before
   * any change.
   * 
   */
  private static final String BENCHMARKTEST_HOSTNAME = "regulus";

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 101 * TupleBatch.BATCH_SIZE + 1;
  private final static int NUM_RUNS = 5;
  private List<ConnectionInfo> connections = null;

  @BeforeClass
  public static void loadSpecificTestData() {
    final File file = new File(JDBC_PASSWORD_PATH);
    if (!file.exists()) {
      throw new RuntimeException("Unable to read " + JDBC_PASSWORD_PATH
          + ". Copy it from /projects/db7/passwords/jdbc_password.txt .");
    }

    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      jdbcPassword = br.readLine();
      br.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to read " + JDBC_PASSWORD_PATH
          + ". Copy it from /projects/db7/passwords/jdbc_password.txt .");
    }

    successfulSetup = true;
  }

  @Before
  public void init() {
    assertTrue(successfulSetup);

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    if (schema == null) {
      schema = Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("i1", "i2"));
      relationKey = RelationKey.of("test", "test", "big");
    }
    Random generator = new Random();
    if (buffer == null || buffer.numTuples() < NUM_TUPLES) {
      buffer = new TupleBatchBuffer(schema);
      for (int i = 0; i < NUM_TUPLES; ++i) {
        buffer.putInt(0, generator.nextInt());
        buffer.putInt(1, generator.nextInt());
      }
    }
    connections = new ArrayList<ConnectionInfo>();

    try {
      String tempFilePath = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_systemtests").toString();
      String workingDir = Paths.get(tempFilePath, BENCHMARKTEST_NAME).toString();
      /* The SQLite connection */
      String jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_SQLITE, BENCHMARKTEST_HOSTNAME, workingDir, 0, null,
              null, null);
      connections.add(ConnectionInfo.of("sqlite", jsonConnInfo));

      /* The MySQL connection */
      jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_MYSQL, BENCHMARKTEST_HOSTNAME, workingDir, 0, "myria1",
              jdbcPassword, null);
      // Uncomment the next line to add tests for MySQL. However, be sure that the MySQL service is up and running.
      // connections.add(ConnectionInfo.of(MyriaConstants.STORAGE_SYSTEM_MYSQL, jsonConnInfo));

      /* The PostgreSQL connection */
      jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, BENCHMARKTEST_HOSTNAME, workingDir, 0,
              "myria1", jdbcPassword, null);
      connections.add(ConnectionInfo.of(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, jsonConnInfo));

      /* The MonetDB connection */
      jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_MONETDB, BENCHMARKTEST_HOSTNAME, workingDir, 0, null,
              null, null);
      // Uncomment the next line to add tests for MonetDB. However, be sure that the MonetDB service is up and running.
      // connections.add(ConnectionInfo.of(MyriaConstants.STORAGE_SYSTEM_MONETDB, jsonConnInfo));

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void benchmarkInsertTest() throws Exception {
    assertTrue(successfulSetup);

    double t1 = 0, t2 = 0;
    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting insert tests with DBMS: {}", conn.getDbms());
      t1 = System.nanoTime();
      TupleSource source = new TupleSource(buffer);
      DbInsert insert = new DbInsert(source, relationKey, conn);
      insert.open(null);
      while (!insert.eos()) {
        insert.nextReady();
      }
      insert.close();
      t2 = System.nanoTime();
      LOGGER.info("Insertion time: {}s", (t2 - t1) / 1000000000.0);
    }
  }

  @Test
  public void benchmarkSelectStarTest() throws Exception {
    assertTrue(successfulSetup);

    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting SELECT * tests with DBMS: {}", conn.getDbms());
      double t1 = System.nanoTime();
      double t2 = 0;
      for (int i = 0; i < NUM_RUNS; i++) {
        DbQueryScan scan =
            new DbQueryScan(conn, "SELECT * FROM " + relationKey.toString(conn.getDbms()), Schema.of(ImmutableList
                .of(Type.LONG_TYPE), ImmutableList.of("count")));
        scan.open(null);
        int count = 0;
        TupleBatch tb = null;
        while (!scan.eos()) {
          tb = scan.nextReady();
          if (tb != null) {
            count += tb.numTuples();
          }
        }
        assertEquals(NUM_TUPLES, count);
        scan.close();
      }
      t2 = System.nanoTime();
      LOGGER.info("Runtime: {}s", (t2 - t1) / (NUM_RUNS * 1000000000.0));
    }
  }

  @Test
  public void benchmarkSelectColumnTest() throws Exception {
    assertTrue(successfulSetup);

    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting ColumnSelect tests with DBMS: {}", conn.getDbms());
      double t1 = System.nanoTime();
      double t2 = 0;
      for (int i = 0; i < NUM_RUNS; i++) {
        DbQueryScan scan =
            new DbQueryScan(conn, "SELECT I1 FROM " + relationKey.toString(conn.getDbms()), Schema.of(ImmutableList
                .of(Type.LONG_TYPE), ImmutableList.of("count")));
        scan.open(null);
        int count = 0;
        TupleBatch tb = null;
        while (!scan.eos()) {
          tb = scan.nextReady();
          if (tb != null) {
            count += tb.numTuples();
          }
        }
        assertEquals(NUM_TUPLES, count);
        scan.close();
      }
      t2 = System.nanoTime();
      LOGGER.info("Runtime: {}s", (t2 - t1) / (NUM_RUNS * 1000000000.0));
    }
  }

  @Test
  public void benchmarkSelectOrderByTest() throws Exception {
    assertTrue(successfulSetup);

    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting ORDER BY tests with DBMS: {}", conn.getDbms());
      double t1 = System.nanoTime();
      double t2 = 0;
      for (int i = 0; i < NUM_RUNS; i++) {
        DbQueryScan scan =
            new DbQueryScan(conn, "SELECT * FROM " + relationKey.toString(conn.getDbms()) + " ORDER BY I1", Schema.of(
                ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("count")));
        scan.open(null);
        int count = 0;
        TupleBatch tb = null;
        while (!scan.eos()) {
          tb = scan.nextReady();
          if (tb != null) {
            count += tb.numTuples();
          }
        }
        assertEquals(NUM_TUPLES, count);
        scan.close();
      }
      t2 = System.nanoTime();
      LOGGER.info("Runtime: {}s", (t2 - t1) / (NUM_RUNS * 1000000000.0));
    }
  }

  @Test
  public void benchmarkSelectSmallJoinTest() throws Exception {
    assertTrue(successfulSetup);

    Integer numExpectedTuples = null;
    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting JOIN (1%) tests with DBMS: {}", conn.getDbms());
      double t1 = System.nanoTime();
      double t2 = 0;
      for (int i = 0; i < NUM_RUNS; i++) {
        DbQueryScan scan =
            new DbQueryScan(conn, "SELECT * FROM " + relationKey.toString(conn.getDbms()) + " R1, "
                + relationKey.toString(conn.getDbms()) + " R2 WHERE R1.I2 < 10000 AND R1.I1 = R2.I2", Schema.of(
                ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("count")));
        scan.open(null);
        int count = 0;
        TupleBatch tb = null;
        while (!scan.eos()) {
          tb = scan.nextReady();
          if (tb != null) {
            count += tb.numTuples();
          }
        }
        if (numExpectedTuples == null) {
          numExpectedTuples = count;
        } else {
          assertEquals(numExpectedTuples, Integer.valueOf(count));
        }
        scan.close();
      }
      t2 = System.nanoTime();
      LOGGER.info("Runtime: {}s", (t2 - t1) / (NUM_RUNS * 1000000000.0));
    }
  }

  @Test
  public void benchmarkSelectSelfJoinTest() throws Exception {
    assertTrue(successfulSetup);

    Integer numExpectedTuples = null;
    for (ConnectionInfo conn : connections) {
      LOGGER.info("Starting JOIN (1%) tests with DBMS: {}", conn.getDbms());
      double t1 = System.nanoTime();
      double t2 = 0;
      for (int i = 0; i < NUM_RUNS; i++) {
        DbQueryScan scan =
            new DbQueryScan(conn, "SELECT * FROM " + relationKey.toString(conn.getDbms()) + " R1, "
                + relationKey.toString(conn.getDbms()) + " R2 WHERE R1.I1 = R2.I2", Schema.of(ImmutableList
                .of(Type.LONG_TYPE), ImmutableList.of("count")));
        scan.open(null);
        int count = 0;
        TupleBatch tb = null;
        while (!scan.eos()) {
          tb = scan.nextReady();
          if (tb != null) {
            count += tb.numTuples();
          }
        }
        if (numExpectedTuples == null) {
          numExpectedTuples = count;
        } else {
          assertEquals(numExpectedTuples, Integer.valueOf(count));
        }
        scan.close();
      }
      t2 = System.nanoTime();
      LOGGER.info("Runtime: {}", (t2 - t1) / (NUM_RUNS * 1000000000.0));
    }
  }
}
