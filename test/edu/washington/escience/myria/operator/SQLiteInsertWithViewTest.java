package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
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

public class SQLiteInsertWithViewTest {

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
    tempDir = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_SQLiteInsertTestWithView");
    tempFile = new File(tempDir.toString(), "SQLiteInsertWithViewTest.db");
    if (!tempFile.exists()) {
      tempFile.createNewFile();
    }
    assertTrue(tempFile.exists());

    /* Create the data needed for the tests in this file. */
    schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE));
    data = new TupleBatchBuffer(schema);
    /* Populate the TupleBatchBuffer. */
    final Random r = new Random();
    for (int i = 0; i < NUM_TUPLES; ++i) {
      data.putInt(0, i);
      data.putInt(1, i);
      data.putString(2, i + "th " + r.nextInt());
    }
  }

  @Test
  public void testViewWithNoCondition() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    final RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");

    final TupleSource source = new TupleSource(data);
    final Schema viewSchema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    final RelationKey actualRelation = RelationKey.of("test", "test", "my_tuple_actual");
    /*
     * Inserts with the relation test#test#my_tuples_actual(col0, col1, col2) with a view test#test#my_tuples(col0,
     * col1) on top.
     * 
     * The view is created with no condition, which should give us all the tuples inserted into the actual relation.
     */
    final DbInsertWithView insert =
        new DbInsertWithView(source, actualRelation, SQLiteInfo.of(tempFile.getAbsolutePath()), tuplesKey, viewSchema,
            null);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    final SQLiteConnection sqliteConnection = new SQLiteConnection(tempFile);
    sqliteConnection.open(false);
    final SQLiteStatement statement =
        sqliteConnection.prepare("SELECT * FROM " + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ";");
    List<String> colNames = viewSchema.getColumnNames();
    for (int i = 0; i < colNames.size(); ++i) {
      assertEquals(colNames.get(i), statement.getColumnName(i));
    }
    assertEquals(viewSchema.numColumns(), statement.columnCount());
    int inserted = 0;
    while (statement.step()) {
      inserted++;
    }
    assertEquals(NUM_TUPLES, inserted);
    sqliteConnection.dispose();
  }

  @Test
  public void testViewWithCondition() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    final RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");
    final TupleSource source = new TupleSource(data);
    final Schema viewSchema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    final RelationKey actualRelation = RelationKey.of("test", "test", "my_tuple_actual");
    final String condition = "COL1 % 2 = 0";
    /*
     * Inserts with the relation test#test#my_tuples_actual(col0, col1, col2) with a view test#test#my_tuples(col0,
     * col1) on top.
     * 
     * The view is created with the condition "COL1 % 2 = 0", which should give us half of the tuples inserted into the
     * actual relation.
     */
    final DbInsertWithView insert =
        new DbInsertWithView(source, actualRelation, SQLiteInfo.of(tempFile.getAbsolutePath()), tuplesKey, viewSchema,
            condition);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    final SQLiteConnection sqliteConnection = new SQLiteConnection(tempFile);
    sqliteConnection.open(false);
    final SQLiteStatement statement =
        sqliteConnection.prepare("SELECT * FROM " + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ";");
    int inserted = 0;
    while (statement.step()) {
      List<String> colNames = viewSchema.getColumnNames();
      for (int i = 0; i < colNames.size(); ++i) {
        assertEquals(colNames.get(i), statement.getColumnName(i));
      }
      assertEquals(viewSchema.numColumns(), statement.columnCount());
      inserted++;
    }
    assertEquals(NUM_TUPLES / 2, inserted);
    sqliteConnection.dispose();
  }

  /**
   * Cleanup what we created.
   * 
   * @throws Exception if setUp fails.
   */
  @After
  public void cleanUp() throws Exception {
    FSUtils.blockingDeleteDirectory(tempDir.toString());
  }

}