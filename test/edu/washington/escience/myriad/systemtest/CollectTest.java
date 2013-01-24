package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

public class CollectTest extends SystemTestBase {

  @Test
  public void collectTest() throws DbException, IOException, CatalogException {
    final String testtableName = "testtable";
    createTable(WORKER_ID[0], testtableName, "id long, name varchar(20)");
    createTable(WORKER_ID[1], testtableName, "id long, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 200, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }

    final TupleBatchBuffer resultTBB = new TupleBatchBuffer(schema);
    resultTBB.merge(tbb);
    resultTBB.merge(tbb);
    final HashMap<Tuple, Integer> expectedResults = TestUtils.tupleBatchToTupleBag(resultTBB);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableName, schema, tb);
      insert(WORKER_ID[1], testtableName, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final SQLiteQueryScan scanTable = new SQLiteQueryScan(null, "select * from " + testtableName, schema);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(WORKER_ID[0], new Operator[] { cp1 });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp1 });

    final Long queryId = 0L;

    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, WORKER_ID);
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = null;
    while ((result = server.startServerQuery(queryId, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    final HashMap<Tuple, Integer> resultSet = TestUtils.tupleBatchToTupleBag(result);

    TestUtils.assertTupleBagEqual(expectedResults, resultSet);

  }
}
