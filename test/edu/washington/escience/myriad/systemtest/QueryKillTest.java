package edu.washington.escience.myriad.systemtest;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.QueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.QueryFuture;
import edu.washington.escience.myriad.parallel.QueryKilledException;
import edu.washington.escience.myriad.util.DelayInjector;
import edu.washington.escience.myriad.util.TestUtils;

public class QueryKillTest extends SystemTestBase {

  @Test(expected = QueryKilledException.class)
  public void killQueryTest() throws Throwable {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], testtableKey, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableKey, "id long, name varchar(20)");

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
        insert(WORKER_ID[0], testtableKey, schema, tb);
        insert(WORKER_ID[1], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
      insert(WORKER_ID[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final QueryScan scanTable = new QueryScan(testtableKey, schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp1 });

    final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, scanTable); // totally delay 10 seconds.
    final CollectProducer cp2 = new CollectProducer(di, serverReceiveID, MASTER_ID);

    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
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
