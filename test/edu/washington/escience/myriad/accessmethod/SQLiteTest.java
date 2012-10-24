package edu.washington.escience.myriad.accessmethod;

import java.util.ArrayList;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteTest {

  @Test
  public void sqliteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "SELECT * FROM testtable";

    final Schema outputSchema =
        new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    /* Scan the testtable in database */
    final SQLiteQueryScan scan = new SQLiteQueryScan(filename, query, outputSchema);

    /* Filter on first column INTEGER >= 50 */
    // Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Long(50), scan);
    /* Filter on first column INTEGER <= 60 */
    // Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Long(60), filter1);

    /* Project onto second column STRING */
    final ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    final ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);
    // Project project = new Project(fieldIdx, fieldType, filter2);

    /* Project is the output operator */
    final Operator root = scan;
    root.open();

    /* For debugging purposes, print Schema */
    final Schema schema = root.getSchema();
    if (schema != null) {
      System.out.println("Schema of result is: " + schema);
    } else {
      System.err.println("Result has no Schema, exiting");
      root.close();
      return;
    }

    /* Print all the results */
    while (root.hasNext()) {
      final _TupleBatch tb = root.next();
      System.out.println(tb);
      // SQLiteAccessMethod.tupleBatchInsert(filename, insert, (TupleBatch) tb);
    }

    /* Cleanup */
    root.close();
  }
}
