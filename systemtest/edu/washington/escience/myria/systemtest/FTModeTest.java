package edu.washington.escience.myria.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.IDBInput;
import edu.washington.escience.myria.operator.LocalJoin;
import edu.washington.escience.myria.operator.Merge;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.Consumer;
import edu.washington.escience.myria.parallel.EOSController;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myria.parallel.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.TestUtils;

public class FTModeTest extends SystemTestBase {
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

  public TupleBatchBuffer generateAMatrix(final RelationKey tableKey, final Schema tableSchema) throws IOException,
      CatalogException, DbException {
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

    createTable(workerIDs[0], tableKey, "follower long, followee long");
    createTable(workerIDs[1], tableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tblAWorker1.popAny()) != null) {
      insert(workerIDs[0], tableKey, tableSchema, tb);
    }
    while ((tb = tblAWorker2.popAny()) != null) {
      insert(workerIDs[1], tableKey, tableSchema, tb);
    }
    return table1;
  }

  public void generateJoinPlan(final ArrayList<ArrayList<RootOperator>> workerPlan, final Schema tableSchema,
      final String initName, final ExchangePairID eoiReceiverOpID, final boolean isHead,
      final ExchangePairID sendingOpID, final ExchangePairID receivingOpID, final ExchangePairID eosReceiverOpID,
      final ExchangePairID serverReceivingOpID, final int selfIDBID) throws DbException {

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    GenericShuffleConsumer sc1;
    if (isHead) {
      ExchangePairID joinArrayID = ExchangePairID.newID();
      final DbQueryScan scan1 = new DbQueryScan(RelationKey.of("test", "test", "r"), tableSchema);
      final GenericShuffleProducer sp1 = new GenericShuffleProducer(scan1, joinArrayID, workerIDs, pf1);
      sc1 = new GenericShuffleConsumer(tableSchema, joinArrayID, workerIDs);
      workerPlan.get(0).add(sp1);
      workerPlan.get(1).add(sp1);
    } else {
      sc1 = new GenericShuffleConsumer(tableSchema, receivingOpID, workerIDs);
    }
    final DbQueryScan scan2 = new DbQueryScan(RelationKey.of("test", "test", initName), tableSchema);
    final ExchangePairID beforeIngress1 = ExchangePairID.newID();
    final GenericShuffleProducer sp2 = new GenericShuffleProducer(scan2, beforeIngress1, workerIDs, pf0);
    final GenericShuffleConsumer sc2 = new GenericShuffleConsumer(tableSchema, beforeIngress1, workerIDs);
    final ExchangePairID beforeIngress2 = ExchangePairID.newID();
    final GenericShuffleProducer sp3_worker1 = new GenericShuffleProducer(null, beforeIngress2, workerIDs, pf0);
    final GenericShuffleProducer sp3_worker2 = new GenericShuffleProducer(null, beforeIngress2, workerIDs, pf0);
    final GenericShuffleConsumer sc3_worker1 = new GenericShuffleConsumer(tableSchema, beforeIngress2, workerIDs);
    final GenericShuffleConsumer sc3_worker2 = new GenericShuffleConsumer(tableSchema, beforeIngress2, workerIDs);
    final Consumer eosReceiver = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpID, new int[] { workerIDs[0] });

    final IDBInput idbinput_worker1 =
        new IDBInput(selfIDBID, eoiReceiverOpID, workerIDs[0], sc2, sc3_worker1, eosReceiver);
    final IDBInput idbinput_worker2 =
        new IDBInput(selfIDBID, eoiReceiverOpID, workerIDs[0], sc2, sc3_worker2, eosReceiver);

    final ExchangePairID[] consumerIDs = new ExchangePairID[] { ExchangePairID.newID(), null, null };
    if (sendingOpID != null) {
      consumerIDs[1] = ExchangePairID.newID();
    }
    if (serverReceivingOpID != null) {
      consumerIDs[2] = ExchangePairID.newID();
    }
    final LocalMultiwayProducer multiProducer_worker1 =
        new LocalMultiwayProducer(idbinput_worker1, removeNull(consumerIDs));
    final LocalMultiwayProducer multiProducer_worker2 =
        new LocalMultiwayProducer(idbinput_worker2, removeNull(consumerIDs));
    final LocalMultiwayConsumer send2join_worker1 = new LocalMultiwayConsumer(tableSchema, consumerIDs[0]);
    final LocalMultiwayConsumer send2join_worker2 = new LocalMultiwayConsumer(tableSchema, consumerIDs[0]);
    LocalMultiwayConsumer send2others_worker1 = null;
    LocalMultiwayConsumer send2others_worker2 = null;
    if (sendingOpID != null) {
      send2others_worker1 = new LocalMultiwayConsumer(tableSchema, consumerIDs[1]);
      send2others_worker2 = new LocalMultiwayConsumer(tableSchema, consumerIDs[1]);
    }
    LocalMultiwayConsumer send2server_worker1 = null;
    LocalMultiwayConsumer send2server_worker2 = null;
    if (serverReceivingOpID != null) {
      send2server_worker1 = new LocalMultiwayConsumer(tableSchema, consumerIDs[2]);
      send2server_worker2 = new LocalMultiwayConsumer(tableSchema, consumerIDs[2]);
    }
    final LocalJoin join_worker1 =
        new LocalJoin(sc1, send2join_worker1, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    final LocalJoin join_worker2 =
        new LocalJoin(sc1, send2join_worker2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    sp3_worker1.setChildren(new Operator[] { join_worker1 });
    sp3_worker2.setChildren(new Operator[] { join_worker2 });

    workerPlan.get(0).addAll(Arrays.asList(new RootOperator[] { multiProducer_worker1, sp2, sp3_worker1 }));
    workerPlan.get(1).addAll(Arrays.asList(new RootOperator[] { multiProducer_worker2, sp2, sp3_worker2 }));

    if (serverReceivingOpID != null) {
      final CollectProducer cp_worker1 = new CollectProducer(send2server_worker1, serverReceivingOpID, MASTER_ID);
      final CollectProducer cp_worker2 = new CollectProducer(send2server_worker2, serverReceivingOpID, MASTER_ID);
      workerPlan.get(0).add(cp_worker1);
      workerPlan.get(1).add(cp_worker2);
    }
    if (sendingOpID != null) {
      final GenericShuffleProducer sp_others_worker1 =
          new GenericShuffleProducer(send2others_worker1, sendingOpID, workerIDs, pf1);
      final GenericShuffleProducer sp_others_worker2 =
          new GenericShuffleProducer(send2others_worker2, sendingOpID, workerIDs, pf1);
      workerPlan.get(0).add(sp_others_worker1);
      workerPlan.get(1).add(sp_others_worker2);
    }
  }

  public static ExchangePairID[] removeNull(final ExchangePairID[] old) {
    int size = 0;
    for (ExchangePairID id : old) {
      if (id != null) {
        size++;
      }
    }
    ExchangePairID[] result = new ExchangePairID[size];
    int idx = 0;
    for (ExchangePairID id : old) {
      if (id != null) {
        result[idx++] = id;
      }
    }
    return result;
  }

  public TupleBatchBuffer getCircularJoinResult(TupleBatchBuffer a, TupleBatchBuffer b, TupleBatchBuffer c,
      Schema schema) {

    final Iterator<List<Column<?>>> iter1 = a.getAllAsRawColumn().iterator();
    boolean s1[][] = new boolean[MaxID][MaxID];
    while (iter1.hasNext()) {
      List<Column<?>> output = iter1.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        s1[fr][fe] = true;
      }
    }
    final Iterator<List<Column<?>>> iter2 = b.getAllAsRawColumn().iterator();
    boolean s2[][] = new boolean[MaxID][MaxID];
    while (iter2.hasNext()) {
      List<Column<?>> output = iter2.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        s2[fr][fe] = true;
      }
    }
    final Iterator<List<Column<?>>> iter3 = c.getAllAsRawColumn().iterator();
    boolean s3[][] = new boolean[MaxID][MaxID];
    while (iter3.hasNext()) {
      List<Column<?>> output = iter3.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        s3[fr][fe] = true;
      }
    }

    while (true) {
      boolean update = false;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (s3[j][i] && s1[i][k] && !s1[j][k]) {
              s1[j][k] = true;
              update = true;
            }
            if (s1[j][i] && s2[i][k] && !s2[j][k]) {
              s2[j][k] = true;
              update = true;
            }
            if (s2[j][i] && s3[i][k] && !s3[j][k]) {
              s3[j][k] = true;
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
        if (s3[i][j]) {
          result.put(0, (long) i);
          result.put(1, (long) j);
          LOGGER.trace(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  @Test
  public void abandonTest() throws Throwable {
    // Using join chain as the test query
    // EDB: A0, B0, C0
    // A := A0
    // B := B0
    // C := C0
    // A := C join A
    // B := A join B
    // C := B join C
    // ans: C

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    // generate the graph
    TupleBatchBuffer a0 = generateAMatrix(RelationKey.of("test", "test", "a0"), tableSchema);
    TupleBatchBuffer b0 = generateAMatrix(RelationKey.of("test", "test", "b0"), tableSchema);
    TupleBatchBuffer c0 = generateAMatrix(RelationKey.of("test", "test", "c0"), tableSchema);

    // generate the correct answer in memory
    final TupleBatchBuffer expectedResult = getCircularJoinResult(a0, b0, c0, tableSchema);

    // the query plan
    final ArrayList<ArrayList<RootOperator>> workerPlan = new ArrayList<ArrayList<RootOperator>>();
    workerPlan.add(new ArrayList<RootOperator>());
    workerPlan.add(new ArrayList<RootOperator>());
    final ExchangePairID eoiReceiverOpID1 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpID2 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpID3 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb1 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb2 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpID_idb3 = ExchangePairID.newID();
    final ExchangePairID receivingAonB = ExchangePairID.newID();
    final ExchangePairID receivingBonC = ExchangePairID.newID();
    final ExchangePairID receivingConA = ExchangePairID.newID();
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    generateJoinPlan(workerPlan, tableSchema, "a0", eoiReceiverOpID1, false, receivingAonB, receivingConA,
        eosReceiverOpID_idb1, null, 0);
    generateJoinPlan(workerPlan, tableSchema, "b0", eoiReceiverOpID2, false, receivingBonC, receivingAonB,
        eosReceiverOpID_idb2, null, 1);
    generateJoinPlan(workerPlan, tableSchema, "c0", eoiReceiverOpID3, false, receivingConA, receivingBonC,
        eosReceiverOpID_idb3, serverReceiveID, 2);

    final Consumer eoiReceiver1 = new Consumer(IDBInput.EOI_REPORT_SCHEMA, eoiReceiverOpID1, workerIDs);
    final Consumer eoiReceiver2 = new Consumer(IDBInput.EOI_REPORT_SCHEMA, eoiReceiverOpID2, workerIDs);
    final Consumer eoiReceiver3 = new Consumer(IDBInput.EOI_REPORT_SCHEMA, eoiReceiverOpID3, workerIDs);
    final Merge merge = new Merge(new Operator[] { eoiReceiver1, eoiReceiver2, eoiReceiver3 });
    final EOSController eosController =
        new EOSController(merge, new ExchangePairID[] {
            eosReceiverOpID_idb1, eosReceiverOpID_idb2, eosReceiverOpID_idb3 }, workerIDs);
    workerPlan.get(0).add(eosController);

    for (List<RootOperator> plan : workerPlan) {
      for (RootOperator root : plan) {
        Operator[] children = root.getChildren();
        final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, children[0], true);
        children[0] = di;
        root.setChildren(children);
      }
    }

    HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs());
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs());
    workerPlans.get(workerIDs[0]).setFTMode("abandon");
    workerPlans.get(workerIDs[1]).setFTMode("abandon");
    for (int i = 0; i < workerPlan.get(0).size(); ++i) {
      workerPlans.get(workerIDs[0]).addRootOp(workerPlan.get(0).get(i));
    }
    for (int i = 0; i < workerPlan.get(1).size(); ++i) {
      workerPlans.get(workerIDs[1]).addRootOp(workerPlan.get(1).get(i));
    }

    final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverReceiveID, workerIDs);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(queueStore));
    serverPlan.setFTMode("abandon");

    QueryFuture qf = server.submitQuery("", "", serverPlan, workerPlans);
    Thread.sleep(2000);
    /* kill the one without EOSController */
    LOGGER.info("killing worker 2!");
    workerProcess[1].destroy();
    qf.sync();
    Preconditions.checkArgument(qf.isSuccess());
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      TupleBatch tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    LOGGER.info("actual: " + actualResult.numTuples() + " expected: " + expectedResult.numTuples());
    Preconditions.checkState(actualResult.numTuples() < expectedResult.numTuples());
  }
}
