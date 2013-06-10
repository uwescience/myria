package edu.washington.escience.myriad.mrbenchmarks;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;

public interface QueryPlanGenerator extends Serializable {

  Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception;

  SinkRoot getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) throws Exception;
}
