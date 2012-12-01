package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.table._TupleBatch;
import edu.washington.escience.myriad.util.SQLiteUtils;
import edu.washington.escience.myriad.util.TestUtils;

public class SQLiteAccessMethodTest {
  @Test
  public void testConcurrentReadingATable() throws IOException, CatalogException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 100000;
    int numThreads = r.nextInt(50) + 1;

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final File dbFile = File.createTempFile(Server.SYSTEM_NAME + "_sqlite_access_method_test", ".db");
    SystemTestBase.createTable(dbFile.getAbsolutePath(), "testtable", "id long, name varchar(20)");

    _TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      String insertTemplate = SQLiteUtils.insertStatementFromSchema(schema, "testtable");
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate, (TupleBatch) tb);
    }

    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          Iterator<TupleBatch> it =
              SQLiteAccessMethod.tupleBatchIteratorFromQuery(dbFile.getAbsolutePath(), "select * from testtable",
                  schema);

          while (it.hasNext()) {
            System.out.println("Thread #" + j + " get a TupleBatch");
            it.next();
          }
        }
      };
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    dbFile.deleteOnExit();
  }

  @Test
  public void testConcurrentReadingTwoTablesInSameDBFile() throws IOException, CatalogException, InterruptedException {

    Random r = new Random();

    int totalRestrict = 100000;
    int numThreads = r.nextInt(2) + 1;

    int numTuplesEach = totalRestrict / numThreads;

    String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final File dbFile = File.createTempFile(Server.SYSTEM_NAME + "_sqlite_access_method_test", ".db");
    System.out.println(dbFile.getAbsolutePath());
    SystemTestBase.createTable(dbFile.getAbsolutePath(), "testtable0", "id long, name varchar(20)");
    SystemTestBase.createTable(dbFile.getAbsolutePath(), "testtable1", "id long, name varchar(20)");

    _TupleBatch tb = null;
    String insertTemplate0 = SQLiteUtils.insertStatementFromSchema(schema, "testtable0");
    String insertTemplate1 = SQLiteUtils.insertStatementFromSchema(schema, "testtable1");
    while ((tb = tbb.popAny()) != null) {
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate0, (TupleBatch) tb);
      SQLiteAccessMethod.tupleBatchInsert(dbFile.getAbsolutePath(), insertTemplate1, (TupleBatch) tb);
    }

    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          Iterator<TupleBatch> it =
              SQLiteAccessMethod.tupleBatchIteratorFromQuery(dbFile.getAbsolutePath(), "select * from testtable" + j
                  % 2, schema);

          while (it.hasNext()) {
            System.out.println("Thread #" + j + " get a TupleBatch");
            it.next();
          }
        }
      };
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    dbFile.deleteOnExit();
  }
}
