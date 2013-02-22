package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class MultipleIDBTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 200;
  private final int numTbl1Worker1 = 100;
  private final int numTbl1Worker2 = 200;

  public TupleBatchBuffer getAJoinResult(TupleBatchBuffer startWith, TupleBatchBuffer multiWith, Schema schema) {

    final Iterator<List<Column<?>>> iter1 = startWith.getAllAsRawColumn().iterator();
    boolean s[][] = new boolean[MaxID][MaxID];
    while (iter1.hasNext()) {
      List<Column<?>> output = iter1.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        s[fr][fe] = true;
      }
    }
    final Iterator<List<Column<?>>> iter2 = multiWith.getAllAsRawColumn().iterator();
    boolean r[][] = new boolean[MaxID][MaxID];
    while (iter2.hasNext()) {
      List<Column<?>> output = iter2.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        r[fr][fe] = true;
      }
    }

    while (true) {
      boolean update = false;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (r[j][i] && s[i][k] && !s[j][k]) {
              s[j][k] = true;
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
        if (s[i][j]) {
          result.put(0, (long) i);
          result.put(1, (long) j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  public TupleBatchBuffer generateAMatrix(final String tableName, final Schema tableSchema) throws IOException,
      CatalogException {
    long[] tblAID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tblAID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    long[] tblAID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tblAID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    TupleBatchBuffer tblAWorker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tblAWorker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tblAWorker1.put(0, tblAID1Worker1[i]);
      tblAWorker1.put(1, tblAID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tblAWorker2.put(0, tblAID1Worker2[i]);
      tblAWorker2.put(1, tblAID2Worker2[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tblAWorker1);
    table1.merge(tblAWorker2);

    createTable(WORKER_ID[0], tableName, "follower long, followee long");
    createTable(WORKER_ID[1], tableName, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tblAWorker1.popAny()) != null) {
      insert(WORKER_ID[0], tableName, tableSchema, tb);
    }
    while ((tb = tblAWorker2.popAny()) != null) {
      insert(WORKER_ID[1], tableName, tableSchema, tb);
    }
    return table1;
  }

  public void generateJoinPlan(final ArrayList<ArrayList<Operator>> workerPlan, final Schema tableSchema,
      final Schema joinSchema, final String initName, final ExchangePairID eoiReceiverOpID, final boolean isHead,
      final boolean isTail, final ExchangePairID sendingOpID, final ExchangePairID receivingOpID,
      final ExchangePairID eosReceiverOpID, final int selfIDBID) throws DbException {

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    final ShuffleConsumer sc1;
    if (isHead) {
      ExchangePairID joinArrayID = ExchangePairID.newID();
      final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from r", tableSchema);
      final ShuffleProducer sp1 = new ShuffleProducer(scan1, joinArrayID, WORKER_ID, pf1);
      sc1 = new ShuffleConsumer(tableSchema, joinArrayID, WORKER_ID);
      workerPlan.get(0).add(sp1);
      workerPlan.get(1).add(sp1);
    } else {
      sc1 = new ShuffleConsumer(tableSchema, receivingOpID, WORKER_ID);
    }
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from " + initName, tableSchema);
    final ExchangePairID beforeIngress1 = ExchangePairID.newID();
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, beforeIngress1, WORKER_ID, pf0);
    final ShuffleConsumer sc2 = new ShuffleConsumer(tableSchema, beforeIngress1, WORKER_ID);
    final ExchangePairID beforeIngress2 = ExchangePairID.newID();
    final ShuffleProducer sp3_worker1 = new ShuffleProducer(null, beforeIngress2, WORKER_ID, pf0);
    final ShuffleProducer sp3_worker2 = new ShuffleProducer(null, beforeIngress2, WORKER_ID, pf0);
    final ShuffleConsumer sc3_worker1 = new ShuffleConsumer(tableSchema, beforeIngress2, WORKER_ID);
    final ShuffleConsumer sc3_worker2 = new ShuffleConsumer(tableSchema, beforeIngress2, WORKER_ID);
    final Consumer eosReceiver = new Consumer(getEOSReportSchema(), eosReceiverOpID, new int[] { WORKER_ID[0] });
    final IDBInput idbinput_worker1 =
        new IDBInput(tableSchema, WORKER_ID[0], selfIDBID, eoiReceiverOpID, WORKER_ID[0], sc2, sc3_worker1, eosReceiver);
    final IDBInput idbinput_worker2 =
        new IDBInput(tableSchema, WORKER_ID[1], selfIDBID, eoiReceiverOpID, WORKER_ID[0], sc2, sc3_worker2, eosReceiver);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayProducer multiProducer_worker1 =
        new LocalMultiwayProducer(idbinput_worker1, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[0]);
    final LocalMultiwayProducer multiProducer_worker2 =
        new LocalMultiwayProducer(idbinput_worker2, new ExchangePairID[] { consumerID1, consumerID2 }, WORKER_ID[1]);
    final LocalMultiwayConsumer send2join_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID1, WORKER_ID[0]);
    final LocalMultiwayConsumer send2join_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID1, WORKER_ID[1]);
    final LocalMultiwayConsumer send2others_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID2, WORKER_ID[0]);
    final LocalMultiwayConsumer send2others_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID2, WORKER_ID[1]);
    final LocalJoin join_worker1 = new LocalJoin(joinSchema, sc1, send2join_worker1, new int[] { 1 }, new int[] { 0 });
    final LocalJoin join_worker2 = new LocalJoin(joinSchema, sc1, send2join_worker2, new int[] { 1 }, new int[] { 0 });
    final Project proj_worker1 = new Project(new Integer[] { 0, 3 }, join_worker1);
    final Project proj_worker2 = new Project(new Integer[] { 0, 3 }, join_worker2);
    sp3_worker1.setChildren(new Operator[] { proj_worker1 });
    sp3_worker2.setChildren(new Operator[] { proj_worker2 });

    workerPlan.get(0).addAll(Arrays.asList(new Operator[] { multiProducer_worker1, sp2, sp3_worker1 }));
    workerPlan.get(1).addAll(Arrays.asList(new Operator[] { multiProducer_worker2, sp2, sp3_worker2 }));

    if (isTail) {
      final CollectProducer cp_worker1 = new CollectProducer(send2others_worker1, sendingOpID, MASTER_ID);
      final CollectProducer cp_worker2 = new CollectProducer(send2others_worker2, sendingOpID, MASTER_ID);
      workerPlan.get(0).add(cp_worker1);
      workerPlan.get(1).add(cp_worker2);
    } else {
      final ShuffleProducer sp_others_worker1 = new ShuffleProducer(send2others_worker1, sendingOpID, WORKER_ID, pf1);
      final ShuffleProducer sp_others_worker2 = new ShuffleProducer(send2others_worker2, sendingOpID, WORKER_ID, pf1);
      workerPlan.get(0).add(sp_others_worker1);
      workerPlan.get(1).add(sp_others_worker2);
    }
  }

  @Test
  public void joinChain() throws DbException, CatalogException, IOException {
    // EDB: R, A0, B0
    // A := A0
    // A := R join A
    // B := B0
    // B := A join B
    // C := C0
    // C := B join C
    // ans: C

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final ImmutableList<Type> joinTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> joinColumnNames = ImmutableList.of("follower", "followee", "follower", "followee");
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    // generate the graph
    TupleBatchBuffer r = generateAMatrix("R", tableSchema);
    TupleBatchBuffer a0 = generateAMatrix("A0", tableSchema);
    TupleBatchBuffer b0 = generateAMatrix("B0", tableSchema);
    TupleBatchBuffer c0 = generateAMatrix("C0", tableSchema);

    // generate the correct answer in memory
    TupleBatchBuffer a = getAJoinResult(a0, r, tableSchema);
    TupleBatchBuffer b = getAJoinResult(b0, a, tableSchema);
    TupleBatchBuffer c = getAJoinResult(c0, b, tableSchema);
    HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(c);

    // the query plan
    final ArrayList<ArrayList<Operator>> workerPlan = new ArrayList<ArrayList<Operator>>();
    workerPlan.add(new ArrayList<Operator>());
    workerPlan.add(new ArrayList<Operator>());
    final ExchangePairID eoiReceiverOpID = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb1 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb2 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb3 = ExchangePairID.newID();
    final ExchangePairID receivingAonB = ExchangePairID.newID();
    final ExchangePairID receivingBonC = ExchangePairID.newID();
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    generateJoinPlan(workerPlan, tableSchema, joinSchema, "a0", eoiReceiverOpID, true, false, receivingAonB, null,
        eosReceiverOpID_idb1, 0);
    generateJoinPlan(workerPlan, tableSchema, joinSchema, "b0", eoiReceiverOpID, false, false, receivingBonC,
        receivingAonB, eosReceiverOpID_idb2, 1);
    generateJoinPlan(workerPlan, tableSchema, joinSchema, "c0", eoiReceiverOpID, false, true, serverReceiveID,
        receivingBonC, eosReceiverOpID_idb3, 2);

    final Consumer eoiReceiver = new Consumer(getEOIReportSchema(), eoiReceiverOpID, WORKER_ID);
    final EOSController eosController =
        new EOSController(eoiReceiver, eoiReceiverOpID, new ExchangePairID[] {
            eosReceiverOpID_idb1, eosReceiverOpID_idb2, eosReceiverOpID_idb3 }, WORKER_ID);
    workerPlan.get(0).add(eosController);

    HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[workerPlan.get(0).size()]);
    workerPlans.put(WORKER_ID[1], new Operator[workerPlan.get(1).size()]);
    for (int i = 0; i < workerPlan.get(0).size(); ++i) {
      workerPlans.get(WORKER_ID[0])[i] = workerPlan.get(0).get(i);
    }
    for (int i = 0; i < workerPlan.get(1).size(); ++i) {
      workerPlans.get(WORKER_ID[1])[i] = workerPlan.get(1).get(i);
    }

    final Long queryId = 0L;
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    final CollectConsumer serverPlan = new CollectConsumer(tableSchema, serverReceiveID, WORKER_ID);
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);
    final HashMap<Tuple, Integer> actual = TestUtils.tupleBatchToTupleBag(result);
    System.out.println(b.numTuples() + " " + result.numTuples());
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
