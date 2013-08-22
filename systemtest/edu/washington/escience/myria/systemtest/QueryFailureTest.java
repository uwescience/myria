package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.failures.CleanupFailureInjector;
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.operator.failures.InitFailureInjector;
import edu.washington.escience.myria.operator.failures.SingleRandomFailureInjector;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.util.TestUtils;

public class QueryFailureTest extends SystemTestBase {

  @Test(expected = DbException.class, timeout = 50000)
  public void workerInitFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.
    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final InitFailureInjector srfi = new InitFailureInjector(scanTable);
    final CollectProducer cp2 = new CollectProducer(srfi, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void masterInitFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.
    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final InitFailureInjector srfi = new InitFailureInjector(serverCollect);

    final SinkRoot serverPlan = new SinkRoot(srfi);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void workerAndMasterInitFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.

    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final InitFailureInjector srfi = new InitFailureInjector(scanTable);
    final CollectProducer cp2 = new CollectProducer(srfi, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final InitFailureInjector srfiMaster = new InitFailureInjector(serverCollect);

    final SinkRoot serverPlan = new SinkRoot(srfiMaster);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);

    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void workerCleanupFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.

    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final CleanupFailureInjector srfi = new CleanupFailureInjector(scanTable);
    final CollectProducer cp2 = new CollectProducer(srfi, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void masterCleanupFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final CleanupFailureInjector srfi = new CleanupFailureInjector(serverCollect);

    final SinkRoot serverPlan = new SinkRoot(srfi);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void workerAndMasterCleanupFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.

    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final CleanupFailureInjector srfi = new CleanupFailureInjector(scanTable);
    final CollectProducer cp2 = new CollectProducer(srfi, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final CleanupFailureInjector srfiMaster = new CleanupFailureInjector(serverCollect);

    final SinkRoot serverPlan = new SinkRoot(srfiMaster);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);

    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 50 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void workerPartitionFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(10, TimeUnit.SECONDS, scanTable); // totally delay 100 seconds.

    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final SingleRandomFailureInjector srfi = new SingleRandomFailureInjector(0, TimeUnit.SECONDS, 1.0, scanTable);
    final CollectProducer cp2 = new CollectProducer(srfi, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 5 seconds,
                                                   // worker0 should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
    System.currentTimeMillis();
  }

  @Test(expected = DbException.class, timeout = 50000)
  public void masterPartitionFailureTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 10;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(workerIDs[0], testtableKey, schema, tb);
        insert(workerIDs[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DbQueryScan scanTable = new DbQueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, scanTable); // totally delay 10 seconds.
    final CollectProducer cp1 = new CollectProducer(di, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleRandomFailureInjector srfi = new SingleRandomFailureInjector(2, TimeUnit.SECONDS, 1.0, serverCollect);

    final SinkRoot serverPlan = new SinkRoot(srfi);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.awaitUninterruptibly(50, TimeUnit.SECONDS); // wait 5 seconds,
                                                   // workers should have
                                                   // completed.
    try {
      qf.sync();
    } catch (DbException e) {
      e.getCause().printStackTrace();
      throw e.getCause();
    }
  }
}
