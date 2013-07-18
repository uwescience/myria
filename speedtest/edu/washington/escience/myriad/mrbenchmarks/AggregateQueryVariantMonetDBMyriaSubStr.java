package edu.washington.escience.myriad.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.QueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.operator.agg.SingleGroupByAggregateNoBuffer;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalShuffleConsumer;
import edu.washington.escience.myriad.parallel.LocalShuffleProducer;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class AggregateQueryVariantMonetDBMyriaSubStr implements QueryPlanGenerator {

  private static final long serialVersionUID = 1L;
  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("sourceIPAddr", "sum_adRevenue");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final QueryScan localScan =
        new QueryScan("select sourceIPAddr, SUM(adRevenue) from UserVisits group by sourceIPAddr", outputSchema);

    final int NUM_LOCAL_TASKS = 5;

    PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    PartitionFunction<String, Integer> pfLocal0 = new SingleFieldHashPartitionFunction(NUM_LOCAL_TASKS);
    pfLocal0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    ExchangePairID[] localShuffleIDs = new ExchangePairID[NUM_LOCAL_TASKS];
    for (int i = 0; i < localShuffleIDs.length; i++) {
      localShuffleIDs[i] = ExchangePairID.newID();
    }
    LocalShuffleProducer localsp = new LocalShuffleProducer(localScan, localShuffleIDs, pfLocal0);

    LocalShuffleConsumer[] lsc = new LocalShuffleConsumer[localShuffleIDs.length];
    final ShuffleProducer[] shuffleLocalGroupBys = new ShuffleProducer[lsc.length];
    final ExchangePairID shuffleLocalGroupByID = ExchangePairID.newID();

    for (int i = 0; i < lsc.length; i++) {
      lsc[i] = new LocalShuffleConsumer(localsp.getSchema(), localShuffleIDs[i]);

      SubStr ss = new SubStr(0, 1, 7);
      ss.setChildren(new Operator[] { lsc[i] });

      final SingleGroupByAggregateNoBuffer localAgg =
          new SingleGroupByAggregateNoBuffer(ss, new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_SUM });

      shuffleLocalGroupBys[i] = new ShuffleProducer(localAgg, shuffleLocalGroupByID, allWorkers, pf0);
    }

    final ShuffleConsumer sc =
        new ShuffleConsumer(shuffleLocalGroupBys[0].getSchema(), shuffleLocalGroupByID, allWorkers);

    final SingleGroupByAggregateNoBuffer agg =
        new SingleGroupByAggregateNoBuffer(sc, new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_SUM });

    final CollectProducer sendToMaster = new CollectProducer(agg, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    RootOperator[] roots = new RootOperator[shuffleLocalGroupBys.length + 2];
    System.arraycopy(shuffleLocalGroupBys, 0, roots, 0, shuffleLocalGroupBys.length);
    roots[shuffleLocalGroupBys.length] = localsp;
    roots[shuffleLocalGroupBys.length + 1] = sendToMaster;

    for (int worker : allWorkers) {
      result.put(worker, roots);
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
