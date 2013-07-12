/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.JdbcInsert;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.TupleSource;

public class VerticaJdbcAccessMethodTest {

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE + 1;

  /* Connection information */
  private final String host = "dbserver01.cs.washington.edu";
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
      schema = Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("value"));
      relationKey = RelationKey.of("test", "test", "big");
    }
    if (buffer == null || buffer.numTuples() < NUM_TUPLES) {
      buffer = new TupleBatchBuffer(schema);
      for (int i = 0; i < NUM_TUPLES; ++i) {
        buffer.put(0, i);
      }
    }
  }

  @Test
  public void testCreateTableAndCountMultipleBatches() throws Exception {
    /* Insert the NUM_TUPLES tuples */
    TupleSource source = new TupleSource(buffer);
    JdbcInsert insert = new JdbcInsert(source, relationKey, jdbcInfo);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    /* Count them and make sure we got the right count. */
    JdbcQueryScan count =
        new JdbcQueryScan(jdbcInfo, "SELECT COUNT(*) FROM " + relationKey.toString(jdbcInfo.getDbms()), Schema.of(
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
}