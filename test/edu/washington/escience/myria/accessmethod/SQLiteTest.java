package edu.washington.escience.myria.accessmethod;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.FSUtils;
import edu.washington.escience.myria.util.SQLiteUtils;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class SQLiteTest {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SQLiteTest.class);

  @Test
  public void sqliteTest() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final Schema outputSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final String tempDirPath =
        Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_SQLiteTest")
            .toFile()
            .getAbsolutePath();
    final String dbAbsolutePath = FilenameUtils.concat(tempDirPath, "sqlite_testtable.db");
    SQLiteUtils.createTable(dbAbsolutePath, testtableKey, "id long,name varchar(20)", true, true);

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(outputSchema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    for (final TupleBatch tb : tbb.getAll()) {
      SQLiteAccessMethod.tupleBatchInsert(SQLiteInfo.of(dbAbsolutePath), testtableKey, tb);
    }

    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(tbb);

    /* Scan the testtable in database */
    final DbQueryScan scan =
        new DbQueryScan(
            SQLiteInfo.of(dbAbsolutePath),
            testtableKey,
            outputSchema,
            new int[] {0, 1},
            new boolean[] {true, false});

    final Operator root = scan;
    root.open(null);

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

    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(result);

    List<TupleBatch> batches = result.getAll();

    Long previousId = null;
    String previousName = null;
    for (TupleBatch tb1 : batches) {
      for (int i = 0; i < tb1.numTuples(); i++) {
        long currentId = tb1.getLong(0, i);
        String currentName = tb1.getString(1, i);
        if (previousId != null) {
          assertNotNull(previousId);
          assertNotNull(previousName);
          assertTrue(previousId <= currentId);
          if (previousId == currentId) {
            assertTrue(previousName.compareTo(currentName) >= 0);
          }
        }
        previousId = currentId;
        previousName = currentName;
      }
    }

    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

    FSUtils.blockingDeleteDirectory(tempDirPath);
  }
}
