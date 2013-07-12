package edu.washington.escience.myriad.accessmethod;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;
import edu.washington.escience.myriad.util.FSUtils;
import edu.washington.escience.myriad.util.SQLiteUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class SQLiteTest {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SQLiteTest.class);

  @Test
  public void sqliteTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final String tempDirPath =
        Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_SQLiteTest").toFile().getAbsolutePath();
    final String dbAbsolutePath = FilenameUtils.concat(tempDirPath, "sqlite_testtable.db");
    SQLiteUtils.createTable(dbAbsolutePath, testtableKey, "id long,name varchar(20)", true, true);

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

    /* Scan the testtable in database */
    final SQLiteQueryScan scan = new SQLiteQueryScan(testtableKey, outputSchema);

    HashMap<String, Object> sqliteFilename = new HashMap<String, Object>();
    sqliteFilename.put("sqliteFile", dbAbsolutePath);
    final ImmutableMap<String, Object> execEnvVars = ImmutableMap.copyOf(sqliteFilename);

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
    root.open(execEnvVars);

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
    while (!root.eos()) {
      tb = root.nextReady();
      if (tb != null) {
        tb.compactInto(result);
      }
    }

    /* Cleanup */
    root.close();

    final HashMap<SystemTestBase.Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(result);

    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

    FSUtils.blockingDeleteDirectory(tempDirPath);

  }
}
