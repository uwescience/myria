package edu.washington.escience.myria.sp2bench;

import java.util.Map;

import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

public interface QueryPlanGenerator {

  Map<Integer, SingleQueryPlanWithArgs> getWorkerPlan(int[] allWorkers) throws Exception;

  RootOperator getMasterPlan(int[] allWorkers) throws Exception;
}
