package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConditionalExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.LocalMultiwayConsumer;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class IDBSyncModeTest extends SystemTestBase {

  private final int MaxID = 10;
  private final int delayPerTuple = 100;
  private final Schema table1Schema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("number"));
  private final ExchangePairID arrayId1 = ExchangePairID.newID();
  private final ExchangePairID arrayId2 = ExchangePairID.newID();
  private final ExchangePairID serverOpId = ExchangePairID.newID();
  private final ExchangePairID eoiReceiverOpId = ExchangePairID.newID();
  private final ExchangePairID eosReceiverOpId = ExchangePairID.newID();

  public List<RootOperator> generatePlan(final boolean sync, final boolean putEosController) {
    final int numPartition = 2;
    final ExchangePairID mpId1 = ExchangePairID.newID();
    final ExchangePairID mpId2 = ExchangePairID.newID();
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);

    final DbQueryScan scan1 = new DbQueryScan(RelationKey.of("test", "test", "r"), table1Schema);
    final DelayInjector di = new DelayInjector(delayPerTuple, TimeUnit.MILLISECONDS, scan1);
    final GenericShuffleProducer sp1 = new GenericShuffleProducer(di, arrayId1, workerIDs, pf0);
    final GenericShuffleConsumer sc1 = new GenericShuffleConsumer(table1Schema, arrayId1, workerIDs);
    final GenericShuffleConsumer sc3 = new GenericShuffleConsumer(table1Schema, arrayId2, workerIDs);
    final Consumer eosReceiver = new Consumer(Schema.EMPTY_SCHEMA, eosReceiverOpId, new int[] { workerIDs[0] });
    final IDBController idbController =
        new IDBController(0, eoiReceiverOpId, workerIDs[0], sc1, sc3, eosReceiver, new DupElim(), sync);
    final LocalMultiwayProducer mp = new LocalMultiwayProducer(idbController, new ExchangePairID[] { mpId1, mpId2 });
    final LocalMultiwayConsumer mc1 = new LocalMultiwayConsumer(table1Schema, mpId1);
    final LocalMultiwayConsumer mc2 = new LocalMultiwayConsumer(table1Schema, mpId2);
    final CollectProducer cp = new CollectProducer(mc1, serverOpId, MASTER_ID);

    /* an expression that adds one to the tuples if they are smaller than MaxID. */
    ImmutableList.Builder<Expression> Expressions = ImmutableList.builder();
    ExpressionOperator var = new VariableExpression(0);
    ExpressionOperator maxId = new ConstantExpression(MaxID);
    ExpressionOperator zero = new ConstantExpression(0);
    ExpressionOperator one = new ConstantExpression(1);
    Expression expr =
        new Expression("number", new ConditionalExpression(new LessThanExpression(var, maxId), new PlusExpression(var,
            one), new PlusExpression(var, zero)));
    Expressions.add(expr);
    final Apply apply = new Apply(mc2, Expressions.build());

    final GenericShuffleProducer sp3 = new GenericShuffleProducer(apply, arrayId2, workerIDs, pf0);
    List<RootOperator> ret = new ArrayList<RootOperator>();
    ret.add(sp1);
    ret.add(sp3);
    ret.add(mp);
    ret.add(cp);
    if (putEosController) {
      final Consumer eoiReceiver = new Consumer(IDBController.EOI_REPORT_SCHEMA, eoiReceiverOpId, workerIDs);
      final UnionAll union = new UnionAll(new Operator[] { eoiReceiver });
      final EOSController eosController = new EOSController(union, new ExchangePairID[] { eosReceiverOpId }, workerIDs);
      ret.add(eosController);
    }
    return ret;
  }

  @Test
  public void syncTest() throws Exception {
    // data generation
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(table1Schema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(table1Schema);
    for (int i = 0; i < MaxID / 2; i++) {
      tbl1Worker1.putLong(0, i);
    }
    for (int i = MaxID / 2; i < MaxID; i++) {
      tbl1Worker2.putLong(0, i);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(table1Schema);
    table1.unionAll(tbl1Worker1);
    table1.unionAll(tbl1Worker2);
    createTable(workerIDs[0], RelationKey.of("test", "test", "r"), "number long");
    createTable(workerIDs[1], RelationKey.of("test", "test", "r"), "number long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(workerIDs[0], RelationKey.of("test", "test", "r"), table1Schema, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(workerIDs[1], RelationKey.of("test", "test", "r"), table1Schema, tb);
    }

    CollectConsumer serverCollect = new CollectConsumer(table1Schema, serverOpId, workerIDs);
    LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SubQueryPlan serverPlan = new SubQueryPlan(new EmptySink(queueStore));

    /* query 1: sync IDBController */
    List<RootOperator> plan1 = generatePlan(true, true);
    List<RootOperator> plan2 = generatePlan(true, false);
    HashMap<Integer, SubQueryPlan> workerPlans = new HashMap<Integer, SubQueryPlan>();
    workerPlans.put(workerIDs[0], new SubQueryPlan(plan1.toArray(new RootOperator[plan1.size()])));
    workerPlans.put(workerIDs[1], new SubQueryPlan(plan2.toArray(new RootOperator[plan2.size()])));

    QueryFuture qf1 = server.getQueryManager().submitQuery("", "", "", serverPlan, workerPlans);
    // sleep for about half of the query execution time.
    Thread.sleep(MaxID * delayPerTuple / 2);
    // get number of received TBs before the query finishes.
    // sync mode blocks until the scan of the initial EDB finished, so no tuple should be received.
    assert (receivedTupleBatches.size() == 0);
    qf1.get();
    TupleBatchBuffer actualResult1 = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult1);
      }
    }

    serverCollect = new CollectConsumer(table1Schema, serverOpId, workerIDs);
    receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    serverPlan = new SubQueryPlan(new EmptySink(queueStore));

    /* query 2: async IDBController */
    plan1 = generatePlan(false, true);
    plan2 = generatePlan(false, false);
    workerPlans.put(workerIDs[0], new SubQueryPlan(plan1.toArray(new RootOperator[plan1.size()])));
    workerPlans.put(workerIDs[1], new SubQueryPlan(plan2.toArray(new RootOperator[plan2.size()])));

    QueryFuture qf2 = server.getQueryManager().submitQuery("", "", "", serverPlan, workerPlans);
    // sleep for about half of the query execution time.
    Thread.sleep(MaxID * delayPerTuple / 2);
    // get number of received TBs before the query finishes.
    // async mode doesn't block, so there should be some tuples received.
    assertTrue(receivedTupleBatches.size() > 0);
    qf2.get();
    TupleBatchBuffer actualResult2 = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult2);
      }
    }

    /* the result should be distinct numbers in [0..MaxID]. */
    assertTrue(actualResult1.numTuples() == MaxID + 1);
    /* two modes should give you the same result. */
    assertTrue(actualResult1.numTuples() == actualResult2.numTuples());
  }
}
