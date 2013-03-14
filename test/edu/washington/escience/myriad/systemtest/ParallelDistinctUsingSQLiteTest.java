package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.BlockingSQLiteDataReceiver;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

public class ParallelDistinctUsingSQLiteTest extends SystemTestBase {

  @Test
  public void parallelTestSQLite() throws DbException, IOException, CatalogException {
    final ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    final RelationKey temptableKey = RelationKey.of("test", "test", "temptable");

    createTable(WORKER_ID[0], testtableKey, "id int, name varchar(20)");
    createTable(WORKER_ID[1], testtableKey, "id int, name varchar(20)");
    createTable(WORKER_ID[0], temptableKey, "id int, name varchar(20)");
    createTable(WORKER_ID[1], temptableKey, "id int, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 20, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.distinct(tbb);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
      insert(WORKER_ID[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final SQLiteQueryScan scan =
        new SQLiteQueryScan(null, "select distinct * from " + testtableKey.toString("sqlite"), schema);
    final CollectProducer cp = new CollectProducer(scan, worker2ReceiveID, WORKER_ID[1]);

    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    final CollectConsumer cc = new CollectConsumer(cp.getSchema(), worker2ReceiveID, WORKER_ID);
    final BlockingSQLiteDataReceiver block2 =
        new BlockingSQLiteDataReceiver(null, RelationKey.of("test", "test", "temptable"), cc);
    final SQLiteSQLProcessor scan22 =
        new SQLiteSQLProcessor(null, "select distinct * from " + temptableKey.toString("sqlite"), schema,
            new Operator[] { block2 });
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp, cp22 });

    server.dispatchWorkerQueryPlans(0L, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[1] });
    TupleBatchBuffer result = server.startServerQuery(0L, serverPlan);

    final HashMap<Tuple, Integer> resultSet = TestUtils.tupleBatchToTupleBag(result);

    TestUtils.assertTupleBagEqual(expectedResult, resultSet);

  }
}
