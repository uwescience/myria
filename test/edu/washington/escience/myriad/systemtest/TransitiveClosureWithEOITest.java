package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.EOSController;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class TransitiveClosureWithEOITest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 400;
  private final int numTbl1Worker1 = 500;
  private final int numTbl1Worker2 = 600;

  public TupleBatchBuffer getResultInMemory(TupleBatchBuffer table1, Schema schema) {
    // a brute force check

    final Iterator<List<Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      List<Column<?>> output = tbs.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        graph[fr][fe] = true;
      }
    }

    while (true) {
      boolean update = false;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (graph[j][i] && graph[i][k] && !graph[j][k]) {
              graph[j][k] = true;
              update = true;
            }
          }
        }
      }
      if (!update) {
        break;
      }
    }

    TupleBatchBuffer result = new TupleBatchBuffer(schema);
    for (int i = 0; i < MaxID; ++i) {
      for (int j = 0; j < MaxID; ++j) {
        if (graph[i][j]) {
          result.put(0, (long) i);
          result.put(1, (long) j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  @Test
  public void transitiveClosure() throws DbException, CatalogException, IOException {
    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final ImmutableList<Type> joinTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> joinColumnNames = ImmutableList.of("follower", "followee", "follower", "followee");
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    // generate the graph
    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.put(0, tbl1ID1Worker2[i]);
      tbl1Worker2.put(1, tbl1ID2Worker2[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    createTable(WORKER_ID[0], "testtable", "follower long, followee long");
    createTable(WORKER_ID[1], "testtable", "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(WORKER_ID[0], "testtable", tableSchema, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(WORKER_ID[1], "testtable", tableSchema, tb);
    }

    // generate the correct answer in memory
    TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema);
    HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from testtable", tableSchema);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    ExchangePairID joinArray1ID = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, joinArray1ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(tableSchema, joinArray1ID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    ExchangePairID beforeIngress1 = ExchangePairID.newID();
    final ShuffleProducer sp2 =
        new ShuffleProducer(scan2, beforeIngress1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(tableSchema, beforeIngress1, new int[] { WORKER_ID[0], WORKER_ID[1] });

    ExchangePairID beforeIngress2 = ExchangePairID.newID();
    final ShuffleProducer sp3_worker1 =
        new ShuffleProducer(null, beforeIngress2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleProducer sp3_worker2 =
        new ShuffleProducer(null, beforeIngress2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    // set their children later
    final ShuffleConsumer sc3_worker1 =
        new ShuffleConsumer(tableSchema, beforeIngress2, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc3_worker2 =
        new ShuffleConsumer(tableSchema, beforeIngress2, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final ExchangePairID eosReceiverOpID = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpID = ExchangePairID.newID();
    final Consumer eosReceiver = new Consumer(getEOSReportSchema(), eosReceiverOpID, new int[] { WORKER_ID[0] });
    final IDBInput idbinput_worker1 =
        new IDBInput(tableSchema, WORKER_ID[0], 0, eoiReceiverOpID, WORKER_ID[0], sc2, sc3_worker1, eosReceiver);
    final IDBInput idbinput_worker2 =
        new IDBInput(tableSchema, WORKER_ID[1], 0, eoiReceiverOpID, WORKER_ID[0], sc2, sc3_worker2, eosReceiver);

    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayProducer multiProducer_worker1 =
        new LocalMultiwayProducer(idbinput_worker1, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[0]);
    final LocalMultiwayProducer multiProducer_worker2 =
        new LocalMultiwayProducer(idbinput_worker2, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[1]);
    final LocalMultiwayConsumer send2join_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID1, WORKER_ID[0]);
    final LocalMultiwayConsumer send2join_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID1, WORKER_ID[1]);
    final LocalMultiwayConsumer send2server_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID2, WORKER_ID[0]);
    final LocalMultiwayConsumer send2server_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID2, WORKER_ID[1]);

    final LocalJoin join_worker1 = new LocalJoin(joinSchema, sc1, send2join_worker1, new int[] { 1 }, new int[] { 0 });
    final LocalJoin join_worker2 = new LocalJoin(joinSchema, sc1, send2join_worker2, new int[] { 1 }, new int[] { 0 });
    final Project proj_worker1 = new Project(new Integer[] { 0, 3 }, join_worker1);
    final Project proj_worker2 = new Project(new Integer[] { 0, 3 }, join_worker2);
    sp3_worker1.setChildren(new Operator[] { proj_worker1 });
    sp3_worker2.setChildren(new Operator[] { proj_worker2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp_worker1 = new CollectProducer(send2server_worker1, serverReceiveID, MASTER_ID);
    final CollectProducer cp_worker2 = new CollectProducer(send2server_worker2, serverReceiveID, MASTER_ID);

    final Consumer eoiReceiver = new Consumer(getEOIReportSchema(), eoiReceiverOpID, WORKER_ID);
    final EOSController eosController =
        new EOSController(eoiReceiver, eoiReceiverOpID, new ExchangePairID[] { eosReceiverOpID }, WORKER_ID);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] {
        cp_worker1, multiProducer_worker1, sp1, sp2, sp3_worker1, eosController });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp_worker2, multiProducer_worker2, sp1, sp2, sp3_worker2 });

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final Long queryId = 0L;
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(result);
    TestUtils.assertTupleBagEqual(expectedResult, actual);
  }

  public Schema getEOIReportSchema() {
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("idbID", "workerID", "numNewTuples");
    final Schema schema = new Schema(types, columnNames);
    return schema;
  }

  public Schema getEOSReportSchema() {
    final ImmutableList<Type> types = ImmutableList.of(Type.BOOLEAN_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("EOS");
    final Schema schema = new Schema(types, columnNames);
    return schema;
  }
}
