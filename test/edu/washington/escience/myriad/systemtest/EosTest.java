package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.EosConsumer;
import edu.washington.escience.myriad.parallel.EosProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.Server;

public class EosTest extends SystemTestBase {

  @Test
  public void collectTest() throws DbException, IOException, CatalogException {
    final Map<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final EosProducer ep = new EosProducer(null, serverReceiveID, MASTER_ID);
    for (int workerId : WORKER_ID) {
      workerPlans.put(workerId, ep);
    }

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
        LOGGER.debug("waiting for server");
      } catch (final InterruptedException e) {
      }
    }

    final EosConsumer serverPlan = new EosConsumer(serverReceiveID, WORKER_ID);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    LOGGER.debug("Query dispatched to the workers");

    while (!Server.runningInstance.startServerQuery(0, serverPlan)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    assertTrue(serverPlan.eos());
  }
}
