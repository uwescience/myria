package edu.washington.escience.myria.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

public class SelectQuerySQLite implements QueryPlanGenerator {

  /**
   * 
   */
  private static final long serialVersionUID = 1742284247760373620L;
  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("pageURL", "pageRank");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, SingleQueryPlanWithArgs> getWorkerPlan(int[] allWorkers) throws Exception {

    // SELECT pageURL, pageRank FROM Rankings WHERE pageRank > X;
    final DbQueryScan selectPageRank =
        new DbQueryScan("select pageURL, pageRank from Rankings where pageRank > 10 ", outputSchema);

    final CollectProducer sendToMaster = new CollectProducer(selectPageRank, sendToMasterID, 0);

    final Map<Integer, SingleQueryPlanWithArgs> result = new HashMap<Integer, SingleQueryPlanWithArgs>();
    for (int worker : allWorkers) {
      result.put(worker, new SingleQueryPlanWithArgs(new RootOperator[] { sendToMaster }));
    }

    return result;
  }

  @Override
  public SinkRoot getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect = new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    SinkRoot serverPlan = new SinkRoot(serverCollect);
    return serverPlan;
  }
}
