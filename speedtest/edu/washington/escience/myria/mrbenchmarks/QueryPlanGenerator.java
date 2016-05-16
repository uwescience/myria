package edu.washington.escience.myria.mrbenchmarks;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.storage.TupleBatch;

public interface QueryPlanGenerator extends Serializable {

  Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception;

  SinkRoot getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception;
}
