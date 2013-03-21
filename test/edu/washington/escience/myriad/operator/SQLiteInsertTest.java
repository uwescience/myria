package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
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
      Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
      final RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");

      final TupleSource source = new TupleSource(data);
      HashMap<String, Object> sqliteFile = new HashMap<String, Object>();
      sqliteFile.put("sqliteFile", tempFile.getAbsolutePath());
      final ImmutableMap<String, Object> execEnvVars = ImmutableMap.copyOf(sqliteFile);
      final SQLiteInsert insert = new SQLiteInsert(source, tuplesKey, true);
      insert.open(execEnvVars);
      while (insert.next() != null) {
      }
      insert.close();

      final SQLiteConnection sqliteConnection = new SQLiteConnection(tempFile);
      sqliteConnection.open(false);
      final SQLiteStatement statement =
          sqliteConnection.prepare("SELECT COUNT(*) FROM " + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)
              + ";");
      assertTrue(statement.step());
      final int inserted = statement.columnInt(0);
      assertTrue(inserted == NUM_TUPLES);
    } finally {
      if (tempFile != null && tempFile.exists()) {
        tempFile.deleteOnExit();
      }
    }
  }
}
