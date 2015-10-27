package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.CastExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.RandomExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.TypeExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.SymmetricHashJoin.JoinPullOrder;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.HashUtils;

public class WriteBufferExpandTest extends SystemTestBase {

  @Override
  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    m.put(1, new SocketInfo(DEFAULT_WORKER_STARTING_PORT));
    m.put(2, new SocketInfo(DEFAULT_WORKER_STARTING_PORT + 1));
    return m;
  }

  @Override
  public Map<String, String> getRuntimeConfigurations() {
    /* Set these parameters small enough to trigger output unavailable. */
    HashMap<String, String> configurations = new HashMap<String, String>();
    configurations.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES, "10");
    configurations.put(MyriaSystemConfigKeys.FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES, "11");
    configurations.put(MyriaSystemConfigKeys.TCP_SEND_BUFFER_SIZE_BYTES, "10");
    configurations.put(MyriaSystemConfigKeys.TCP_RECEIVE_BUFFER_SIZE_BYTES, "10");
    configurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "1");
    configurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER, "0");
    return configurations;
  }

  /** Need to be big enough to make sure we can get output unavailable. */
  private final int MaxN = 500000;
  /** The first value. */
  private final int value1 = 1;
  /** The second value. */
  private final int value2 = -1;

  @Test
  public void writeBufferExtendTest() throws Exception {
    /*
     * Two IDBControllers to form a circle. Each IDBController is followed by a join, which already has MaxN tuples in
     * its left (EDB) child before pulling from the IDBController. The pull will generate ~MaxN/2 tuples which will be
     * put in the join buffer, and (hopefully) enough to make the state of the fragment to be output unavailable before
     * the iterative input of the other IDBController is consumed. When both fragments have output unavailable and
     * waiting for the other one to consume, we need {@link WorkerSubQuery.writeBufferExpandTimer} to extend the write
     * buffers to solve the deadlock.
     */
    final Schema tableSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("x"));
    TupleBatchBuffer testHash = new TupleBatchBuffer(tableSchema);
    testHash.putLong(0, value1);
    testHash.putLong(0, value2);
    TupleBatch testTB = testHash.popAny();
    int h1 = (HashUtils.hashValue(testTB, 0, 0, 0) + 2) % 2;
    int h2 = (HashUtils.hashValue(testTB, 0, 1, 0) + 2) % 2;
    /*
     * We need to evenly distribute the join result tuples to two workers (to block everyone), so make sure we have two
     * values that will be sent to different workers. If this assertion fails, change the value pair (1, -1) to some
     * other values that can pass.
     */
    assertTrue(h1 != h2);

    TupleBatchBuffer table = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < MaxN; i++) {
      table.putLong(0, i % 2 == 0 ? value1 : value2);
    }
    createTable(workerIDs[0], RelationKey.of("test", "test", "c"), "x long");
    createTable(workerIDs[1], RelationKey.of("test", "test", "c"), "x long");
    TupleBatch tb = null;
    while ((tb = table.popAny()) != null) {
      insert(workerIDs[0], RelationKey.of("test", "test", "c"), tableSchema, tb);
      insert(workerIDs[1], RelationKey.of("test", "test", "c"), tableSchema, tb);
    }

    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(2, 0);
    final ExchangePairID joinArrayId1 = ExchangePairID.newID();
    final ExchangePairID joinArrayId2 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpId1 = ExchangePairID.newID();
    final ExchangePairID eoiReceiverOpId2 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpId1 = ExchangePairID.newID();
    final ExchangePairID eosReceiverOpId2 = ExchangePairID.newID();

    // the query plan
    final DbQueryScan scan1 = new DbQueryScan(RelationKey.of("test", "test", "c"), tableSchema);
    final GenericShuffleConsumer sc1 = new GenericShuffleConsumer(tableSchema, joinArrayId2, workerIDs);
    final Consumer eosReceiver1 = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpId1, new int[] { workerIDs[0] });
    final IDBController idbController1 =
        new IDBController(0, eoiReceiverOpId1, workerIDs[0], scan1, sc1, eosReceiver1, new DupElim());
    final DbQueryScan scan2 = new DbQueryScan(RelationKey.of("test", "test", "c"), tableSchema);
    final SymmetricHashJoin join1 =
        new SymmetricHashJoin(scan2, idbController1, new int[] { 0 }, new int[] { 0 }, new int[] { 0 }, new int[] {},
            false, false);
    join1.setOpName("join1");
    join1.setPullOrder(JoinPullOrder.LEFT_EOS);

    ImmutableList.Builder<Expression> expressions1 = ImmutableList.builder();
    expressions1.add(new Expression("x", new MinusExpression(new CastExpression(new TimesExpression(
        new RandomExpression(), new ConstantExpression(2)), new TypeExpression(Type.LONG_TYPE)),
        new ConstantExpression(1))));
    Apply apply1 = new Apply(join1, expressions1.build());

    final GenericShuffleProducer sp1 = new GenericShuffleProducer(apply1, joinArrayId1, workerIDs, pf0);

    final DbQueryScan scan3 = new DbQueryScan(RelationKey.of("test", "test", "c"), tableSchema);
    final GenericShuffleConsumer sc2 = new GenericShuffleConsumer(tableSchema, joinArrayId1, workerIDs);
    final Consumer eosReceiver2 = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpId2, new int[] { workerIDs[0] });
    final IDBController idbController2 =
        new IDBController(1, eoiReceiverOpId2, workerIDs[0], scan3, sc2, eosReceiver2, new DupElim());
    final DbQueryScan scan4 = new DbQueryScan(RelationKey.of("test", "test", "c"), tableSchema);
    final SymmetricHashJoin join2 =
        new SymmetricHashJoin(scan4, idbController2, new int[] { 0 }, new int[] { 0 }, new int[] { 0 }, new int[] {},
            false, false);
    join2.setOpName("join2");
    join2.setPullOrder(JoinPullOrder.LEFT_EOS);

    ImmutableList.Builder<Expression> expressions2 = ImmutableList.builder();
    expressions2.add(new Expression("x", new MinusExpression(new CastExpression(new TimesExpression(
        new RandomExpression(), new ConstantExpression(2)), new TypeExpression(Type.LONG_TYPE)),
        new ConstantExpression(1))));
    Apply apply2 = new Apply(join2, expressions2.build());

    final GenericShuffleProducer sp2 = new GenericShuffleProducer(apply2, joinArrayId2, workerIDs, pf0);

    final Consumer eoiReceiver1 = new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpId1, workerIDs);
    final Consumer eoiReceiver2 = new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpId2, workerIDs);
    final UnionAll union = new UnionAll(new Operator[] { eoiReceiver1, eoiReceiver2 });
    final EOSController eosController =
        new EOSController(union, new ExchangePairID[] { eosReceiverOpId1, eosReceiverOpId2 }, workerIDs);

    List<RootOperator> plan = new ArrayList<RootOperator>();
    plan.add(sp1);
    plan.add(sp2);
    HashMap<Integer, SubQueryPlan> workerPlans = new HashMap<Integer, SubQueryPlan>();
    workerPlans.put(workerIDs[1], new SubQueryPlan(plan.toArray(new RootOperator[plan.size()])));
    plan.add(eosController);
    workerPlans.put(workerIDs[0], new SubQueryPlan(plan.toArray(new RootOperator[plan.size()])));
    // final ExchangePairID serverOpId = ExchangePairID.newID();
    // final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverOpId, workerIDs);
    // final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    // final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SubQueryPlan serverPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));
    server.getQueryManager().submitQuery("", "", "", serverPlan, workerPlans).get();
  }
}
