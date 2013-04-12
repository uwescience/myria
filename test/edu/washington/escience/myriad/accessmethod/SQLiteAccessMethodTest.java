package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.util.SQLiteUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class SQLiteAccessMethodTest {
  @Test
  public void testConcurrentReadingATable() throws IOException, CatalogException, InterruptedException {

    final Random r = new Random();

    final int totalRestrict = 100000;
    final int numThreads = r.nextInt(50) + 1;
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final File dbFile = File.createTempFile(MyriaConstants.SYSTEM_NAME + "_sqlite_access_method_test", ".db");
    SystemTestBase.createTable(dbFile.getAbsolutePath(), testtableKey, "id long, name varchar(20)");

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      final String insertTemplate = SQLiteUtils.insertStatementFromSchema(schema, testtableKey);
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate, tb);
    }

    final Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          final Iterator<TupleBatch> it =
              SQLiteAccessMethod.tupleBatchIteratorFromQuery(dbFile.getAbsolutePath(), "select * from "
                  + testtableKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);
          while (it.hasNext()) {
            it.next();
          }
        }
      };
      threads[i].setName("SQLiteAccessMethodTest-" + i);
    }

    for (final Thread t : threads) {
      t.start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    dbFile.deleteOnExit();
  }

  @Test
  public void testConcurrentReadingTwoTablesInSameDBFile() throws IOException, CatalogException, InterruptedException {

    final Random r = new Random();

    final int totalRestrict = 100000;
    final int numThreads = r.nextInt(2) + 1;

    final List<RelationKey> testtableKeys = new ArrayList<RelationKey>();
    final RelationKey testtable0Key = RelationKey.of("test", "test", "testtable0");
    final RelationKey testtable1Key = RelationKey.of("test", "test", "testtable1");
    testtableKeys.add(testtable0Key);
    testtableKeys.add(testtable1Key);

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final File dbFile = File.createTempFile(MyriaConstants.SYSTEM_NAME + "_sqlite_access_method_test", ".db");
    SystemTestBase.createTable(dbFile.getAbsolutePath(), testtable0Key, "id long, name varchar(20)");
    SystemTestBase.createTable(dbFile.getAbsolutePath(), testtable1Key, "id long, name varchar(20)");

    TupleBatch tb = null;
    final String insertTemplate0 = SQLiteUtils.insertStatementFromSchema(schema, testtable0Key);
    final String insertTemplate1 = SQLiteUtils.insertStatementFromSchema(schema, testtable1Key);
    while ((tb = tbb.popAny()) != null) {
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate0, tb);
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate1, tb);
    }

    final Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          final Iterator<TupleBatch> it =
              SQLiteAccessMethod.tupleBatchIteratorFromQuery(dbFile.getAbsolutePath(), "select * from "
                  + testtableKeys.get(j % 2).toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);

          while (it.hasNext()) {
            it.next();
          }
        }
      };
      threads[i].setName("SQLiteAccessMethodTest-" + i);
    }

    for (final Thread t : threads) {
      t.start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    dbFile.deleteOnExit();
  }
}
