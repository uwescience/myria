package edu.washington.escience.myria.accessmethod;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.FSUtils;
import edu.washington.escience.myria.util.SQLiteUtils;
import edu.washington.escience.myria.util.TestUtils;

public class SQLiteAccessMethodTest {
  @Test
  public void testConcurrentReadingATable() throws Exception {

    final Random r = new Random();

    final int totalRestrict = 100000;
    final int numThreads = r.nextInt(50) + 1;
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    final int numTuplesEach = totalRestrict / numThreads;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuplesEach, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    Path tempDir =
        Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_sqlite_access_method_test");
    final File dbFile = new File(tempDir.toString(), "sqlite.db");
    SQLiteUtils.createTable(
        dbFile.getAbsolutePath(), testtableKey, "id long, name varchar(20)", true, true);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      SQLiteAccessMethod.tupleBatchInsert(
          SQLiteInfo.of(dbFile.getAbsolutePath()), testtableKey, tb);
    }

    final Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] =
          new Thread() {
            @Override
            public void run() {
              try {
                final Iterator<TupleBatch> it =
                    SQLiteAccessMethod.tupleBatchIteratorFromQuery(
                        SQLiteInfo.of(dbFile.getAbsolutePath()),
                        "select * from "
                            + testtableKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
                        schema);
                while (it.hasNext()) {
                  it.next();
                }
              } catch (DbException e) {
                // TODO Do some error condition here. Talk to dhalperi
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
    FSUtils.blockingDeleteDirectory(tempDir.toString());
  }

  @Test
  public void testConcurrentReadingTwoTablesInSameDBFile() throws Exception {

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
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    Path tempDir =
        Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_sqlite_access_method_test");
    final File dbFile = new File(tempDir.toString(), "sqlite.db");
    SQLiteUtils.createTable(
        dbFile.getAbsolutePath(), testtable0Key, "id long, name varchar(20)", true, true);
    SQLiteUtils.createTable(
        dbFile.getAbsolutePath(), testtable1Key, "id long, name varchar(20)", true, true);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      SQLiteAccessMethod.tupleBatchInsert(
          SQLiteInfo.of(dbFile.getAbsolutePath()), testtable0Key, tb);
      SQLiteAccessMethod.tupleBatchInsert(
          SQLiteInfo.of(dbFile.getAbsolutePath()), testtable1Key, tb);
    }

    final Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int j = i;
      threads[i] =
          new Thread() {
            @Override
            public void run() {
              try {
                final Iterator<TupleBatch> it =
                    SQLiteAccessMethod.tupleBatchIteratorFromQuery(
                        SQLiteInfo.of(dbFile.getAbsolutePath()),
                        "select * from "
                            + testtableKeys
                                .get(j % 2)
                                .toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
                        schema);

                while (it.hasNext()) {
                  it.next();
                }
              } catch (DbException e) {
                // TODO Do some error condition here. Talk to dhalperi
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
    FSUtils.blockingDeleteDirectory(tempDir.toString());
  }

  @Test
  public void concurrentlyReadAndWriteTest() throws Exception {

    final int numTuples = 1000000;

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    final long[] tbl1ID1 = TestUtils.randomLong(1, 1000, numTuples);
    final long[] tbl1ID2 = TestUtils.randomLong(1, 1000, numTuples);
    final TupleBatchBuffer tbl1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTuples; i++) {
      tbl1.putLong(0, tbl1ID1[i]);
      tbl1.putLong(1, tbl1ID2[i]);
    }

    Path tempDir =
        Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_sqlite_access_method_test");
    final File dbFile = new File(tempDir.toString(), "sqlite.db");

    final RelationKey inputKey = RelationKey.of("test", "testWrite", "input");
    SQLiteUtils.createTable(
        dbFile.getAbsolutePath(), inputKey, "follower long, followee long", true, true);

    TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      SQLiteAccessMethod.tupleBatchInsert(SQLiteInfo.of(dbFile.getAbsolutePath()), inputKey, tb);
    }

    final RelationKey outputKey = RelationKey.of("test", "testWrite", "output");
    final SQLiteInfo sqliteInfo = SQLiteInfo.of(dbFile.getAbsolutePath());
    final DbQueryScan scan = new DbQueryScan(sqliteInfo, inputKey, tableSchema);
    final DbInsert insert = new DbInsert(scan, outputKey, sqliteInfo, true);
    insert.open(null);
    while (!insert.eos()) {
      insert.nextReady();
    }

    FSUtils.blockingDeleteDirectory(tempDir.toString());
  }
}
