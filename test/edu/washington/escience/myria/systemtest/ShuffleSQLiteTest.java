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
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.ShuffleConsumer;
import edu.washington.escience.myria.parallel.ShuffleProducer;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.util.TestUtils;

public class ShuffleSQLiteTest extends SystemTestBase {

  @Test
  public void shuffleTestSQLite() throws Exception {

    /*
     * Prepare expected result
     * 
     * NOTE: simpleRandomJoinTestBase() have already create and add tuples to testtable1 and testtable2
     */
    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    /* Create table: testtable1, testtable2, temptable1, temptable2 */
    final ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    final RelationKey testtable1Key = RelationKey.of("test", "test", "testtable1");
    final RelationKey testtable2Key = RelationKey.of("test", "test", "testtable2");
    final RelationKey temptable1Key = RelationKey.of("test", "test", "temptable1");
    final RelationKey temptable2Key = RelationKey.of("test", "test", "temptable2");

    createTable(workerIDs[0], temptable1Key, "id int, name varchar(20)");
    createTable(workerIDs[0], temptable2Key, "id int, name varchar(20)");
    createTable(workerIDs[1], temptable1Key, "id int, name varchar(20)");
    createTable(workerIDs[1], temptable2Key, "id int, name varchar(20)");

    final ExchangePairID serverReceiveID = ExchangePairID.newID(); // for CollectOperator
    final ExchangePairID shuffle1ID = ExchangePairID.newID(); // for ShuffleOperator
    final ExchangePairID shuffle2ID = ExchangePairID.newID(); // for ShuffleOperator

    /* Set output schema */
    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    /* Create partition function */
    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(numPartition);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by name

    /* Set shuffle producers, sp1, sp2 , which load data from scan1, scan 2 */
    final DbQueryScan scan1 = new DbQueryScan(testtable1Key, schema);
    final DbQueryScan scan2 = new DbQueryScan(testtable2Key, schema);
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, shuffle1ID, workerIDs, pf);
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, shuffle2ID, workerIDs, pf);

    /* Set shuffle consumers, sc1, sc2, which received data and store as new table temptable1 and 2 */
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1.getSchema(), shuffle1ID, workerIDs);
    final BlockingSQLiteDataReceiver buffer1 = new BlockingSQLiteDataReceiver(temptable1Key, sc1);

    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2.getSchema(), shuffle2ID, workerIDs);
    final BlockingSQLiteDataReceiver buffer2 = new BlockingSQLiteDataReceiver(temptable2Key, sc2);

    /* Set collect producer which will send data inner-joined */
    final SQLiteSQLProcessor ssp =
        new SQLiteSQLProcessor("select * from " + temptable1Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)
            + " inner join " + temptable2Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " on "
            + temptable1Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ".name="
            + temptable2Key.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ".name", outputSchema, new Operator[] {
            buffer1, buffer2 });
    final CollectProducer cp = new CollectProducer(ssp, serverReceiveID, MASTER_ID);

    /* Set worker plans */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp, sp1, sp2 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp, sp1, sp2 });

    /* Prepare collect consumers */
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    /* Submit and execute worker plans */
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
