/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import static org.junit.Assert.assertEquals;

import java.util.Objects;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.LeafOperator;
import edu.washington.escience.myria.operator.TupleRangeSource;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

/**
 * @author dhalperi
 * 
 */
public class JdbcAccessMethodTest {
  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);

  private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
  private static final int MYSQL_PORT = 3306;
  private static final String MYSQL_DATABASE_NAME = "myria_test";
  private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
  private static final int POSTGRES_PORT = 5432;
  private static final String POSTGRES_DATABASE_NAME = "myria_test";

  private JdbcInfo getJdbcInfo(final String dbms) {
    if (TestUtils.inTravis()) {
      /* Return localhost using Travis' default credentials. */
      if (dbms.equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        return JdbcInfo.of(POSTGRES_DRIVER_CLASS, MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, "localhost", POSTGRES_PORT,
            POSTGRES_DATABASE_NAME, "postgres", "");
      } else {
        return JdbcInfo.of(MYSQL_DRIVER_CLASS, MyriaConstants.STORAGE_SYSTEM_MYSQL, "localhost", MYSQL_PORT,
            MYSQL_DATABASE_NAME, "travis", "");
      }
    }

    /* Return the MyriaTest AWS MySQL instance */
    final String host = "54.213.118.143";
    final String user = "myria";
    final String password = "nays26[shark";
    return JdbcInfo.of(MYSQL_DRIVER_CLASS, MyriaConstants.STORAGE_SYSTEM_MYSQL, host, MYSQL_PORT, MYSQL_DATABASE_NAME,
        user, password);
  }

  private void testInsertTuplesAndCountThem(final String dbms) throws DbException {
    final int expectedNumResults = 250;
    TupleRangeSource source = new TupleRangeSource(expectedNumResults, Type.DOUBLE_TYPE);
    doInsert(dbms, source, expectedNumResults);
  }

  private void testInsertTuplesAndCountThemWithNull(final String dbms) throws DbException {
    final int expectedNumResults = 250;
    TupleBatchBuffer data =
        new TupleBatchBuffer(Schema.of(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("value")));
    for (int i = 0; i < expectedNumResults; i++) {
      if (i % 2 == 0) {
        data.putString(0, "");
      } else {
        data.putString(0, String.valueOf(i));
      }
    }
    TupleSource source = new TupleSource(data);
    doInsert(dbms, source, expectedNumResults);
  }

  private void doInsert(final String dbms, final LeafOperator source, final int expectedNumResults) throws DbException {
    Objects.requireNonNull(dbms, "dbms");

    /* Connection information */
    final JdbcInfo jdbcInfo = getJdbcInfo(dbms);

    /* First, insert tuples into the database. */
    final Schema schema = source.getSchema();
    final RelationKey relation = RelationKey.of("myria", "test", dbms);
    DbInsert insert = new DbInsert(source, relation, jdbcInfo, true);
    /* Run to completion. */
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    /* Next get all the tables back out. */
    DbQueryScan scan = new DbQueryScan(jdbcInfo, relation, schema);

    /* Count up the results and assert they match expectations */
    int count = 0;
    scan.open(null);
    TupleBatch tb = null;
    while (!scan.eos()) {
      tb = scan.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    /* Cleanup */
    scan.close();

    /* Test it. */
    assertEquals(expectedNumResults, count);
  }

  @Test
  public void testInsertTuplesAndCountThemMySQL() throws DbException {
    if (!TestUtils.inTravis()) {
      LOGGER.warn("Skipping MySQL test since not in Travis.");
      return;
    }
    testInsertTuplesAndCountThem(MyriaConstants.STORAGE_SYSTEM_MYSQL);
  }

  @Test
  public void testInsertTuplesAndCountThemPostgreSQL() throws DbException {
    if (!TestUtils.inTravis()) {
      LOGGER.warn("Skipping PostgreSQL test since not in Travis.");
      return;
    }
    testInsertTuplesAndCountThem(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);
    testInsertTuplesAndCountThemWithNull(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL);
  }
}
