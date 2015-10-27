package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.failures.DelayInjector;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class FlowControlTest extends SystemTestBase {

  @Override
  public Map<String, String> getRuntimeConfigurations() {
    HashMap<String, String> configurations = new HashMap<String, String>();
    configurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "2");
    configurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER, "1");
    return configurations;
  }

  @Test
  public void flowControlTest() throws Exception {
    TestUtils.skipIfInTravis();
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(workerIDs[0], testtableKey, "id long, name varchar(20)");

    final int numTuples = TupleBatch.BATCH_SIZE * 100;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < numTuples; i++) {
      tbb.putLong(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.putString(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
    }

    final TupleSource ts = new TupleSource(tbb);

    final ExchangePairID worker1ReceiveID = ExchangePairID.newID();
    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(ts, worker1ReceiveID, workerIDs[1]);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });

    final CollectConsumer cc1 = new CollectConsumer(schema, worker1ReceiveID, new int[] { workerIDs[0] });
    final DelayInjector di = new DelayInjector(50, TimeUnit.MILLISECONDS, cc1);
    final CollectProducer cp2 = new CollectProducer(di, serverReceiveID, MASTER_ID);

    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { workerIDs[1] });
    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    assertEquals(numTuples, serverPlan.getCount());

  }
}
