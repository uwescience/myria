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
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.QueryKilledException;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.TestUtils;

public class QueryKillTest extends SystemTestBase {

  @Test(expected = QueryKilledException.class)
  public void killQueryTest() throws Throwable {
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
      tbb.putLong(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.putString(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
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

    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { cp1 }));

    final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, scanTable); // totally delay 10 seconds.
    final CollectProducer cp2 = new CollectProducer(di, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { cp2 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });

    final SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(serverCollect));

    QueryFuture qf = server.submitQueryPlan("", "", "", serverPlan, workerPlans);
    qf.awaitUninterruptibly(5, TimeUnit.SECONDS); // wait 5 seconds,
                                                  // worker0 should have
                                                  // completed.

    server.killQuery(qf.getQuery().getQueryID());
    try {
      qf.sync();
    } catch (DbException e) {
      throw e.getCause();
    }
  }
}
