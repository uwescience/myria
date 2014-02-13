package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.util.FSUtils;

public class SequenceTest {

  private static Path tempDir;
  private static File tempFile;
  private static final int NUM_TUPLES = TupleBatch.BATCH_SIZE + 3;

  /**
   * Setup what we need for the tests in this file.
   * 
   * @throws Exception if setUp fails.
   */
  @BeforeClass
  public static void setUp() throws Exception {
    /* Turn off SQLite logging */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);

    /* Make a temporary file for the database and create a new SQLite database there. */
    tempDir = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_SQLiteInsertTest");
    tempFile = new File(tempDir.toString(), "SQLiteInsertTest.db");
    if (!tempFile.exists()) {
      tempFile.createNewFile();
    }
    assertTrue(tempFile.exists());
  }

  @Test
  public void testInsertThenScan() throws Exception {
    /* Source and insert the relation. */
    TupleRangeSource source = new TupleRangeSource(NUM_TUPLES, Type.INT_TYPE);
    RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");
    DbInsert insert = new DbInsert(source, tuplesKey, SQLiteInfo.of(tempFile.getAbsolutePath()), true);
    /* Then count up the tuples in it. */
    Schema outputSchema = Schema.of(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("cnt"));
    DbQueryScan count =
        new DbQueryScan(SQLiteInfo.of(tempFile.getAbsolutePath()), "SELECT COUNT(*) AS cnt FROM "
            + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ";", outputSchema);
    /* Wrap these in a sequence. */
    Sequence seq = new Sequence(new Operator[] { insert, count });
    assertEquals(outputSchema, seq.getSchema());

    seq.open(null);
    TupleBatch tb = null;
    while (!seq.eos()) {
      tb = seq.nextReady();
      assertTrue(tb == null || seq.nextReady() == null && seq.eos());
    }
    seq.close();

    assertTrue(tb != null);
    assertEquals(1, tb.numTuples());
    assertEquals(outputSchema, tb.getSchema());
    assertEquals(NUM_TUPLES, tb.getLong(0, 0));
  }

  @Test
  public void testNestedSequencesInsertThenScan() throws Exception {
    /* Source and insert the relation. */
    TupleRangeSource source = new TupleRangeSource(NUM_TUPLES, Type.INT_TYPE);
    RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");
    DbInsert insert = new DbInsert(source, tuplesKey, SQLiteInfo.of(tempFile.getAbsolutePath()), true);
    /* Then count up the tuples in it. */
    Schema outputSchema = Schema.of(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("cnt"));
    DbQueryScan count =
        new DbQueryScan(SQLiteInfo.of(tempFile.getAbsolutePath()), "SELECT COUNT(*) AS cnt FROM "
            + tuplesKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ";", outputSchema);
    /* Test a nested collection of sequences. */
    Sequence inner1 = new Sequence(new Operator[] { insert });
    Sequence inner2 = new Sequence(new Operator[] { count });
    Sequence innerA = new Sequence(new Operator[] { inner1, inner2 });
    Sequence seq = new Sequence(new Operator[] { innerA });
    assertEquals(outputSchema, seq.getSchema());

    seq.open(null);
    TupleBatch tb = null;
    while (!seq.eos()) {
      tb = seq.nextReady();
      assertTrue(tb == null || seq.nextReady() == null && seq.eos());
    }
    seq.close();

    assertTrue(tb != null);
    assertEquals(1, tb.numTuples());
    assertEquals(outputSchema, tb.getSchema());
    assertEquals(NUM_TUPLES, tb.getLong(0, 0));
  }

  @Test(expected = NullPointerException.class)
  public void testNullChildren() throws Exception {
    Sequence seq = new Sequence(null);
    seq.open(null);
    seq.fetchNextReady();
    seq.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyChildren() throws Exception {
    Sequence seq = new Sequence(new Operator[] {});
    seq.open(null);
    seq.fetchNextReady();
    seq.close();
  }

  @Test(expected = NullPointerException.class)
  public void testNullChild() throws Exception {
    Sequence seq = new Sequence(new Operator[] { null });
    seq.open(null);
    seq.fetchNextReady();
    seq.close();
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
