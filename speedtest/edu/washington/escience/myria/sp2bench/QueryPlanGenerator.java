package edu.washington.escience.myria.sp2bench;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.storage.TupleBatch;

public interface QueryPlanGenerator {

  Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception;

  RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception;
}
