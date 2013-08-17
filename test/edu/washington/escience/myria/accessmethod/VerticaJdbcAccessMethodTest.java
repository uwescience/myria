/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.systemtest.SystemTestBase.Tuple;
import edu.washington.escience.myria.util.TestUtils;

public class VerticaJdbcAccessMethodTest {

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE + 1;
  private Schema floatTestSchema;
  private RelationKey floatTestRelationKey;
  private TupleBatchBuffer floatTestTb;

  /* Connection information */
  private final String host = "dbserver05.cs.washington.edu";
  private final int port = 15433;
  private final String user = "dbadmin";
  private final String password = "mrbenchmarks";
  private final String dbms = MyriaConstants.STORAGE_SYSTEM_VERTICA;
  private final String databaseName = "mrbenchmarks";
  private final String jdbcDriverName = "com.vertica.jdbc.Driver";
  private final JdbcInfo jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, password);

  @Before
  public void createTupleBatchBuffers() {
    if (schema == null) {
      schema = Schema.of(ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE), ImmutableList.of("value", "name"));
      relationKey = RelationKey.of("test", "test", "big");
    }
    if (buffer == null || buffer.numTuples() < NUM_TUPLES) {
      buffer = new TupleBatchBuffer(schema);
      for (int i = 0; i < NUM_TUPLES; ++i) {
        buffer.put(0, i);
        buffer.put(1, String.valueOf(i) + " test");
      }
    }
  }

  @Before
  public void createFloatTestTb() {
    if (floatTestSchema == null) {
      floatTestSchema =
          new Schema(ImmutableList.of(Type.STRING_TYPE, Type.FLOAT_TYPE), ImmutableList.of("name", "height"));
      floatTestRelationKey = RelationKey.of("shumochu", "verticaTest", "floatConversion");
    }
    if (floatTestTb == null) {
      floatTestTb = new TupleBatchBuffer(floatTestSchema);
      final String[] c1 = TestUtils.randomFixedLengthNumericString(1000, 2000, 500, 20);
      final float[] c2 = TestUtils.randomFloat((float) 140.11, (float) 200.69, 500);
      for (int i = 0; i < c1.length; i++) {
        floatTestTb.put(0, c1[i]);
        floatTestTb.put(1, c2[i]);
      }
    }
  }

  @Test
  public void testCreateTableAndCountMultipleBatches() throws Exception {
    /* Insert the NUM_TUPLES tuples */
    TupleSource source = new TupleSource(buffer);
    DbInsert insert = new DbInsert(source, relationKey, jdbcInfo);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    /* Count them and make sure we got the right count. */
    DbQueryScan count =
        new DbQueryScan(jdbcInfo, "SELECT COUNT(*) FROM " + relationKey.toString(jdbcInfo.getDbms()), Schema.of(
            ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("count")));
    count.open(null);

    TupleBatch result = count.nextReady();
    assertTrue(result != null);
    assertTrue(result.getLong(0, 0) == NUM_TUPLES);
    result = count.nextReady();
    assertTrue(result == null);
    assertTrue(count.eos());
    count.close();
  }

  /** testing the conversion of myria float and vertica float. */
  @Test
  public void testFloatVerticaConversion() throws Exception {

    /* get expected result */
    HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(floatTestTb);

    /* insert tuples. */
    TupleSource source = new TupleSource(floatTestTb);
    DbInsert insert = new DbInsert(source, floatTestRelationKey, jdbcInfo);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    /* read back. */
    DbQueryScan scan =
        new DbQueryScan(jdbcInfo, "SELECT * FROM " + floatTestRelationKey.toString(jdbcInfo.getDbms()), Schema.of(
            ImmutableList.of(Type.STRING_TYPE, Type.FLOAT_TYPE), ImmutableList.of("name", "height")));
    TupleBatchBuffer resultTbb = new TupleBatchBuffer(floatTestSchema);
    scan.open(null);
    TupleBatch resultTb;
    while ((resultTb = scan.nextReady()) != null) {
      TupleBatchBuffer temp = new TupleBatchBuffer(floatTestSchema);
      resultTb.compactInto(temp);
      resultTbb.merge(temp);
    }
    scan.close();

    /* check */
    HashMap<Tuple, Integer> result = TestUtils.tupleBatchToTupleBag(resultTbb);
    TestUtils.assertTupleBagEqual(expectedResult, result);
  }
}