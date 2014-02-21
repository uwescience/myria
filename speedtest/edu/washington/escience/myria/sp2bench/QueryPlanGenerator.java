package edu.washington.escience.myria.sp2bench;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

public interface QueryPlanGenerator {

  Map<Integer, SingleQueryPlanWithArgs> getWorkerPlan(int[] allWorkers) throws Exception;

  RootOperator getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception;
}
