/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import static org.junit.Assert.assertTrue;

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
import edu.washington.escience.myria.util.Constants;

/**
 * @author dhalperi
 * 
 */
public class MonetDbJdbcAccessMethodTest {

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 2 * Constants.getBatchSize() + 1;

  /* Connection information */
  private final String host = "54.213.118.143";
  private final int port = 50000;
  private final String user = "myria";
  private final String password = "nays26[shark";
  private final String dbms = MyriaConstants.STORAGE_SYSTEM_MONETDB;
  private final String databaseName = "myria-test";
  private final String jdbcDriverName = "nl.cwi.monetdb.jdbc.MonetDriver";
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
        buffer.putInt(0, i);
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
}