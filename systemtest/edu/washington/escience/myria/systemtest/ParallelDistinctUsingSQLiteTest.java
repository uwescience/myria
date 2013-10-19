package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.BlockingSQLiteDataReceiver;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SQLiteSQLProcessor;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class ParallelDistinctUsingSQLiteTest extends SystemTestBase {

  @Test
  public void parallelTestSQLite() throws Exception {
    final ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    final RelationKey temptableKey = RelationKey.of("test", "test", "temptable");

    createTable(workerIDs[0], testtableKey, "id int, name varchar(20)");
    createTable(workerIDs[1], testtableKey, "id int, name varchar(20)");
    createTable(workerIDs[0], temptableKey, "id int, name varchar(20)");
    createTable(workerIDs[1], temptableKey, "id int, name varchar(20)");

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, 20, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.distinct(tbb);

    TupleBatch tb = null;
    while ((tb = tbb.popAny()) != null) {
      insert(workerIDs[0], testtableKey, schema, tb);
      insert(workerIDs[1], testtableKey, schema, tb);
    }

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final DbQueryScan scan =
        new DbQueryScan("select distinct * from " + testtableKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);
    final CollectProducer cp = new CollectProducer(scan, worker2ReceiveID, workerIDs[1]);

    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    final CollectConsumer cc = new CollectConsumer(cp.getSchema(), worker2ReceiveID, workerIDs);
    final BlockingSQLiteDataReceiver block2 =
        new BlockingSQLiteDataReceiver(RelationKey.of("test", "test", "temptable"), cc);
    final SQLiteSQLProcessor scan22 =
        new SQLiteSQLProcessor("select distinct * from " + temptableKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE),
            schema, new Operator[] { block2 });
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp, cp22 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

  }
}
