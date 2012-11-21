package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;
import edu.washington.escience.myriad.table._TupleBatch;

public class SQLiteTest {

  @Test
  public void sqliteTest() throws DbException, IOException, CatalogException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    final Schema outputSchema =
        new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    String dbAbsolutePath = SystemTestBase.workerTestBaseFolder + File.separator + "sqlite_testtable.db";
    SystemTestBase.createTable(dbAbsolutePath, "testtable", "id long,name varchar(20)");

    String[] names = SystemTestBase.randomFixedLengthNumericString(1000, 1005, 200, 20);
    long[] ids = SystemTestBase.randomLong(1000, 1005, names.length);

    TupleBatchBuffer tbb = new TupleBatchBuffer(outputSchema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    for (_TupleBatch tb : tbb.getAll()) {
      String insertTemplate = SQLiteInsert.insertStatementFromSchema(outputSchema, "testtable");
      SQLiteAccessMethod.tupleBatchInsert(dbAbsolutePath, insertTemplate, (TupleBatch) tb);
    }

    HashMap<Tuple, Integer> expectedResult = SystemTestBase.tupleBatchToTupleBag(tbb);

    final String query = "SELECT * FROM testtable";

    /* Scan the testtable in database */
    final SQLiteQueryScan scan = new SQLiteQueryScan(new File(dbAbsolutePath).getName(), query, outputSchema);

    scan.setDataDir(new File(dbAbsolutePath).getParent());

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

    _TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(outputSchema);
    while ((tb = root.next()) != null) {
      result.putAll(tb);
    }

    /* Cleanup */
    root.close();

    HashMap<SystemTestBase.Tuple, Integer> resultBag = SystemTestBase.tupleBatchToTupleBag(result);

    SystemTestBase.assertTupleBagEqual(expectedResult, resultBag);

  }
}
