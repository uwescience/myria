package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;
import edu.washington.escience.myriad.util.FSUtils;
import edu.washington.escience.myriad.util.SQLiteUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class SQLiteTest {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SQLiteTest.class.getName());

  @Test
  public void sqliteTest() throws DbException, IOException, CatalogException {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final String tempDirPath = Files.createTempDirectory(Server.SYSTEM_NAME + "_SQLiteTest").toFile().getAbsolutePath();
    final String dbAbsolutePath = FilenameUtils.concat(tempDirPath, "sqlite_testtable.db");
    SystemTestBase.createTable(dbAbsolutePath, testtableKey, "id long,name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(outputSchema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    for (final TupleBatch tb : tbb.getAll()) {
      final String insertTemplate = SQLiteUtils.insertStatementFromSchema(outputSchema, testtableKey);
      SQLiteAccessMethod.tupleBatchInsert(dbAbsolutePath, insertTemplate, tb);
    }

    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(tbb);

    final String query = "SELECT * FROM " + testtableKey.toString("sqlite");

    /* Scan the testtable in database */
    final SQLiteQueryScan scan = new SQLiteQueryScan(new File(dbAbsolutePath).getName(), query, outputSchema);

    scan.setPathToSQLiteDb(dbAbsolutePath);

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
      LOGGER.debug("Schema of result is: " + schema);
    } else {
      LOGGER.error("Result has no Schema, exiting");
      root.close();
      return;
    }

    TupleBatch tb = null;
    final TupleBatchBuffer result = new TupleBatchBuffer(outputSchema);
    while ((tb = root.next()) != null) {
      tb.compactInto(result);
    }

    /* Cleanup */
    root.close();

    final HashMap<SystemTestBase.Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(result);

    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

    FSUtils.blockingDeleteDirectory(tempDirPath);

  }
}
