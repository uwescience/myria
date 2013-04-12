package edu.washington.escience.myriad.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.BlockingSQLiteDataReceiver;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SQLiteSQLProcessor;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class ShuffleSQLiteTest extends SystemTestBase {

  @Test
  public void shuffleTestSQLite() throws Exception {

    final ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    final RelationKey testtable1Key = RelationKey.of("test", "test", "testtable1");
    final RelationKey testtable2Key = RelationKey.of("test", "test", "testtable2");
    final RelationKey temptable1Key = RelationKey.of("test", "test", "temptable1");
    final RelationKey temptable2Key = RelationKey.of("test", "test", "temptable2");

    createTable(WORKER_ID[0], temptable1Key, "id int, name varchar(20)");
    createTable(WORKER_ID[0], temptable2Key, "id int, name varchar(20)");
    createTable(WORKER_ID[1], temptable1Key, "id int, name varchar(20)");
    createTable(WORKER_ID[1], temptable2Key, "id int, name varchar(20)");

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID shuffle1ID = ExchangePairID.newID();
    final ExchangePairID shuffle2ID = ExchangePairID.newID();

    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final int numPartition = 2;

    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    final SQLiteQueryScan scan1 =
        new SQLiteQueryScan("select * from " + testtable1Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);
    final SQLiteQueryScan scan2 =
        new SQLiteQueryScan("select * from " + testtable2Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, WORKER_ID, pf);

    final ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, WORKER_ID, pf);

    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1.getSchema(), shuffle1ID, WORKER_ID);
    final BlockingSQLiteDataReceiver buffer1 = new BlockingSQLiteDataReceiver(temptable1Key, sc1);

    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2.getSchema(), shuffle2ID, WORKER_ID);
    final BlockingSQLiteDataReceiver buffer2 = new BlockingSQLiteDataReceiver(temptable2Key, sc2);

    final SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("select * from " + temptable1Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)
            + " inner join " + temptable2Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " on "
            + temptable1Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ".name="
            + temptable2Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ".name", outputSchema, new Operator[] {
            buffer1, buffer2 });
    final CollectProducer cp = new CollectProducer(ssp, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, sp1, sp2 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp, sp1, sp2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
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
