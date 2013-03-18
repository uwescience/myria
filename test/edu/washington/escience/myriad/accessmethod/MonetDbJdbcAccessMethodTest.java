/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.JdbcInsert;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.TupleSource;

/**
 * @author dhalperi
 * 
 */
public class MonetDbJdbcAccessMethodTest {

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE + 1;

  /* Connection information */
  private final String host = "54.245.108.198";
  private final int port = 50000;
  private final String user = "myria";
  private final String password = "nays26[shark";
  private final String dbms = "monetdb";
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
      insert.next();
    }
    insert.close();

    /* Count them and make sure we got the right count. */
    JdbcQueryScan count =
        new JdbcQueryScan(jdbcInfo, "SELECT COUNT(*) FROM " + relationKey.toString(jdbcInfo.getDbms()), Schema.of(
            ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("count")));
    count.open(null);
    TupleBatch result = count.next();
    assertTrue(result != null);
    assertTrue(result.getLong(0, 0) == NUM_TUPLES);
    result = count.next();
    assertTrue(result == null);
    assertTrue(count.eos());
    count.close();
  }
}