package edu.washington.escience.myriad.sqlite;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class SQLiteInsertSpeedTest {
  private static File tempFile = null;
  private static SQLiteConnection fileConnection;
  private static SQLiteConnection memoryConnection;
  private static int[] ints;
  private static String[] strings;
  private static final int NUM_TUPLES = 1 * 1000 * 1000;

  /**
   * Setup what we need for the tests in this file.
   * 
   * @throws Exception if setUp fails.
   */
  @BeforeClass
  public static void setUp() throws Exception {
    /* Turn off SQLite logging */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.WARNING);

    /* Make a temporary file for the database and create a new SQLite database there. */
    tempFile = File.createTempFile("SQLiteInsertSpeedTest", ".db");
    tempFile.deleteOnExit();
    fileConnection = new SQLiteConnection(tempFile).open();
    memoryConnection = new SQLiteConnection().open();

    /* Create the tables needed for the tests in this file. */
    fileConnection.exec("CREATE TABLE insertTestTable (id INTEGER, name STRING);");
    memoryConnection.exec("CREATE TABLE insertTestTable (id INTEGER, name STRING);");

    /* Create the data needed for the tests in this file. */
    final Random r = new Random();
    ints = new int[NUM_TUPLES];
    strings = new String[NUM_TUPLES];
    for (int i = 0; i < NUM_TUPLES; ++i) {
      ints[i] = i;
      strings[i] = i + "th " + r.nextInt();
    }
  }

  /**
   * Teardown anything we did in setup.
   * 
   * @throws Exception if tearDown fails.
   */
  @AfterClass
  public static void tearDown() throws Exception {
    fileConnection.dispose();
    fileConnection = null;
    memoryConnection.dispose();
    memoryConnection = null;
    /* Nothing for tempFile; it is set to delete on exit. */
  }

  private void doOneRun(final SQLiteConnection sqliteConnection, final int batchSize, final int numTuples) throws SQLiteException {
    /* Drop any existing tuples */
    sqliteConnection.exec("DELETE FROM insertTestTable;");

    /* Prepare the query */
    SQLiteStatement statement = sqliteConnection.prepare("INSERT INTO insertTestTable (id,name) VALUES (?,?);");

    /* Take the start time */
    final Date begin = new Date();

    /* Insert all the tuples */
    for (int i = 0; i < numTuples; i++) {
      /* Start a new transaction every batchSize tuples */
      if (i % batchSize == 0) {
        sqliteConnection.exec("BEGIN TRANSACTION;");
      }

      /* Insert one row */
      statement.bind(1, ints[i]);
      statement.bind(2, strings[i]);
      statement.step();
      statement.reset();

      /* End the transaction every batchSize tuples */
      if ((i + 1) % batchSize == 0) {
        sqliteConnection.exec("COMMIT TRANSACTION;");
      }
    }
    /* Insert any remaining tuples */
    if (numTuples % batchSize != 0) {
      sqliteConnection.exec("COMMIT TRANSACTION;");
    }
    statement.dispose();
    statement = null;

    /* Take the stop time */
    final double totalSeconds = (new Date().getTime() - begin.getTime()) * 1.0 / 1000;

    /* Make sure all the tuples actually got inserted. */
    statement = sqliteConnection.prepare("SELECT COUNT(*) FROM insertTestTable;");
    statement.step();
    assertTrue(statement.columnInt(0) == numTuples);
    statement.dispose();
    statement = null;

    System.out.printf("\t[%d, %d] %.2f seconds in total, %f tuples per second\n", numTuples, batchSize, totalSeconds,
        numTuples / totalSeconds);
  }

  private void doSpeedTest(final String description, final SQLiteConnection sqliteConnection) throws SQLiteException {
    final int[] BATCH_SIZES = new int[] { 100, 1000, 10000, 20000, 50000, 1000000 };
    System.out.printf("About to run test: %s\n", description);
    for (final int batchSize : BATCH_SIZES) {
      doOneRun(sqliteConnection, batchSize, NUM_TUPLES);
    }
  }

  @Test
  public void insertToMemory() {
    assertTrue(fileConnection != null);
    try {
      doSpeedTest("InsertToSQLiteInMemory", memoryConnection);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void insertToTempFile() {
    assertTrue(fileConnection != null);
    try {
      doSpeedTest("InsertToSQLiteInTempFile", fileConnection);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
