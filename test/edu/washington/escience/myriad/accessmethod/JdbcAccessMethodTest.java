/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.JdbcQueryScan;

/**
 * @author dhalperi
 * 
 */
public class JdbcAccessMethodTest {

  @Test
  public void testNumberResultsAndMultipleBatches() throws DbException {
    /* Connection information */
    final String host = "54.245.108.198";
    final int port = 3306;
    final String user = "myriad";
    final String password = "nays26[shark";
    final String dbms = "mysql";
    final String databaseName = "myriad_test";
    final String jdbcDriverName = "com.mysql.jdbc.Driver";
    final int expectedNumResults = 250; /* Hardcoded in setup_testtablebig.sql */
    /* Query information */
    final String query = "select * from testtablebig";
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("value");
    final Schema schema = new Schema(types, columnNames);

    /* Build up the QueryScan parameters and open the scan */
    final String connectionString = "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName;
    final JdbcQueryScan scan = new JdbcQueryScan(jdbcDriverName, connectionString, query, schema, user, password);

    scan.open();

    /* Count up the results and assert they match expectations */
    int count = 0;
    TupleBatch tb = null;
    while ((tb = scan.next()) != null) {
      count += tb.numTuples();
    }
    assertTrue(count == expectedNumResults);

    /* Cleanup */
    scan.close();
  }

}
