package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

public class SQLiteInsertTest {

  private static File tempFile;
  private static final int NUM_TUPLES = 1000;
  private static Schema schema;
  private static TupleBatchBuffer data;

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
    tempFile = File.createTempFile("SQLiteInsertTest", ".db");
    tempFile.deleteOnExit();

    /* Create the data needed for the tests in this file. */
    schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE));
    data = new TupleBatchBuffer(schema);
    /* Populate the TupleBatchBuffer. */
    final Random r = new Random();
    for (int i = 0; i < NUM_TUPLES; ++i) {
      data.put(0, i);
      data.put(1, i + "th " + r.nextInt());
    }
  }

  @Test
  public void test() throws Exception {
    try {
      ExecutorService myExecutor = Executors.newSingleThreadExecutor();
      Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);

      TupleSource source = new TupleSource(data);
      SQLiteInsert insert = new SQLiteInsert(source, "my_tuples", tempFile.getAbsolutePath(), myExecutor, true);
      insert.open();
      while (insert.next() != null) {
      }
      insert.close();

      SQLiteConnection sqliteConnection = new SQLiteConnection(tempFile);
      sqliteConnection.open(false);
      SQLiteStatement statement = sqliteConnection.prepare("SELECT COUNT(*) FROM my_tuples;");
      assertTrue(statement.step());
      int inserted = statement.columnInt(0);
      assertTrue(inserted == NUM_TUPLES);
    } finally {
      if (tempFile != null && tempFile.exists()) {
        tempFile.deleteOnExit();
      }
    }
  }
}
