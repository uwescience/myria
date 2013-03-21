package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.faulttolerance.DelayInjector;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

public class FlowControlTest extends SystemTestBase {

  @Override
  public Map<String, String> getMasterConfigurations() {
    HashMap<String, String> masterConfigurations = new HashMap<String, String>();
    masterConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "1");
    return masterConfigurations;
  }

  @Override
  public Map<String, String> getWorkerConfigurations() {
    HashMap<String, String> workerConfigurations = new HashMap<String, String>();
    workerConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "1");
    return workerConfigurations;
  }

  @Test
  public void flowControlTest() throws Exception {
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], testtableKey, "id long, name varchar(20)");

    final int numTuples = 50000;

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    TupleBatch tb = null;
    int numTB = 0;
    for (int i = 0; i < numTuples; i++) {
      tbb.put(0, TestUtils.randomLong(0, 100000, 1)[0]);
      tbb.put(1, TestUtils.randomFixedLengthNumericString(0, 100000, 1, 20)[0]);
      while ((tb = tbb.popFilled()) != null) {
        LOGGER.debug("Insert a TB into testbed. #" + numTB + ".");
        numTB++;
        insert(WORKER_ID[0], testtableKey, schema, tb);
      }
    }
    if ((tb = tbb.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, schema, tb);
    }

    final ExchangePairID worker1ReceiveID = ExchangePairID.newID();
    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final SQLiteQueryScan scanTable =
        new SQLiteQueryScan("select * from " + testtableKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(scanTable, worker1ReceiveID, WORKER_ID[1]);
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp1 });

    final CollectConsumer cc1 = new CollectConsumer(schema, worker1ReceiveID, new int[] { WORKER_ID[0] });
    final DelayInjector di = new DelayInjector(1, TimeUnit.SECONDS, cc1);
    final CollectProducer cp2 = new CollectProducer(di, serverReceiveID, MASTER_ID);

    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp2 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[1] });
    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    assertEquals(numTuples, serverPlan.getCount());

  }
}
