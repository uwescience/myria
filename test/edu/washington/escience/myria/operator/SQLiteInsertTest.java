package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.FSUtils;

public class SQLiteInsertTest {

  private static Path tempDir;
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
    tempDir = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_SQLiteInsertTest");
    tempFile = new File(tempDir.toString(), "SQLiteInsertTest.db");
    if (!tempFile.exists()) {
      tempFile.createNewFile();
    }
    assertTrue(tempFile.exists());

    /* Create the data needed for the tests in this file. */
    schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE));
    data = new TupleBatchBuffer(schema);
    /* Populate the TupleBatchBuffer. */
    final Random r = new Random();
    for (int i = 0; i < NUM_TUPLES; ++i) {
      data.putInt(0, i);
      data.putString(1, i + "th " + r.nextInt());
    }
  }

  @Test
  public void test() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    final RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");

    final TupleSource source = new TupleSource(data);
    final DbInsert insert =
        new DbInsert(source, tuplesKey, SQLiteInfo.of(tempFile.getAbsolutePath()));
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    final SQLiteConnection sqliteConnection = new SQLiteConnection(tempFile);
    sqliteConnection.open(false);
    final SQLiteStatement statement =
        sqliteConnection.prepare(
            "SELECT COUNT(*) FROM "
                + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)
                + ";");
    assertTrue(statement.step());
    final int inserted = statement.columnInt(0);
    assertEquals(NUM_TUPLES, inserted);
    sqliteConnection.dispose();
  }

  /**
   * Cleanup what we created.
   *
   * @throws Exception if setUp fails.
   */
  @AfterClass
  public static void cleanUp() throws Exception {
    FSUtils.blockingDeleteDirectory(tempDir.toString());
  }
}
