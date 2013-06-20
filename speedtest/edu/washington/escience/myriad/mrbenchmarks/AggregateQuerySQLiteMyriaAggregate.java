package edu.washington.escience.myriad.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.operator.agg.SingleGroupByAggregateNoBuffer;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class AggregateQuerySQLiteMyriaAggregate implements QueryPlanGenerator {

  /**
   * 
   */
  private static final long serialVersionUID = 5147615855012257218L;
  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("sourceIPAddr", "sum_adRevenue");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> scanTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> scanColumnNames = ImmutableList.of("sourceIPAddr", "adRevenue");
  final static Schema scanSchema = new Schema(scanTypes, scanColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final SQLiteQueryScan localScan = new SQLiteQueryScan("select sourceIPAddr, adRevenue from UserVisits", scanSchema);

    final SingleGroupByAggregateNoBuffer localAgg =
        new SingleGroupByAggregateNoBuffer(localScan, new int[] { 1 }, 0,
            new int[] { Aggregator.AGG_OP_SUM });

    final ExchangePairID shuffleLocalGroupByID = ExchangePairID.newID();

    PartitionFunction<String, Integer> pf = new SingleFieldHashPartitionFunction(allWorkers.length);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final ShuffleProducer shuffleLocalGroupBy = new ShuffleProducer(localAgg, shuffleLocalGroupByID, allWorkers, pf);
    final ShuffleConsumer sc = new ShuffleConsumer(shuffleLocalGroupBy.getSchema(), shuffleLocalGroupByID, allWorkers);

    final SingleGroupByAggregateNoBuffer globalAgg =
        new SingleGroupByAggregateNoBuffer(sc, new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_SUM });

    final CollectProducer sendToMaster = new CollectProducer(globalAgg, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    for (int worker : allWorkers) {
      result.put(worker, new RootOperator[] { shuffleLocalGroupBy, sendToMaster });
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
