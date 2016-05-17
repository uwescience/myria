package edu.washington.escience.myria.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class AggregateQueryMonetDBSingleWorker implements QueryPlanGenerator {

  /**
   * .
   */
  private static final long serialVersionUID = -7391407686119481293L;

  final static ImmutableList<Type> outputTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames =
      ImmutableList.of("sourceIPAddr", "sum_adRevenue");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final DbQueryScan localGroupBy =
        new DbQueryScan(
            "select sourceIPAddr, SUM(adRevenue) from UserVisits group by sourceIPAddr",
            outputSchema);

    final CollectProducer sendToMaster = new CollectProducer(localGroupBy, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    for (int worker : allWorkers) {
      result.put(worker, new RootOperator[] {sendToMaster});
    }

    return result;
  }

  @Override
  public SinkRoot getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    SinkRoot serverPlan = new SinkRoot(serverCollect);
    return serverPlan;
  }
}
