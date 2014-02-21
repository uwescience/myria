package edu.washington.escience.myria.mrbenchmarks;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

public class JoinQueryPostgres implements QueryPlanGenerator, Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  final static ImmutableList<Type> scanTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE,
      Type.LONG_TYPE);
  final static ImmutableList<String> scanColumnNames = ImmutableList.of("sourceIPAddr", "sum(adRevenue)",
      "sum(pageRank)", "count(pageRank)");
  final static Schema scanSchema = new Schema(scanTypes, scanColumnNames);

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("sourceIPAddr", "totalAvenue", "avgPageRank");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, SingleQueryPlanWithArgs> getWorkerPlan(int[] allWorkers) throws Exception {

    final DbQueryScan localScan =
        new DbQueryScan( //
            "select sourceIPAddr, sum(adRevenue) as sumAdRevenue, sum(pageRank) as sumPageRank, count(pageRank) as countPageRank"
                + //
                "  from UserVisits, Rankings" + //
                "  where visitDate between date '2000-01-15' and date '2000-01-22'" + //
                "        and Rankings.pageURL=UserVisits.destinationURL" + //
                "  group by sourceIPAddr", scanSchema);

    final ExchangePairID localScanID = ExchangePairID.newID();
    // final ExchangePairID setFilterID = ExchangePairID.newID();
    // final ExchangePairID joinID = ExchangePairID.newID();
    // final ExchangePairID globalAggID = ExchangePairID.newID();

    // shuffle by destURL to get pageRanks
    PartitionFunction pf = new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final GenericShuffleProducer spLocalScan = new GenericShuffleProducer(localScan, localScanID, allWorkers, pf);
    final GenericShuffleConsumer scLocalScan =
        new GenericShuffleConsumer(spLocalScan.getSchema(), localScanID, allWorkers);

    final SingleGroupByAggregate globalAgg =
        new SingleGroupByAggregate(scLocalScan, new int[] { 1, 2, 3 }, 0, new int[] {
            Aggregator.AGG_OP_SUM, Aggregator.AGG_OP_SUM, Aggregator.AGG_OP_SUM });

    GlobalAvg globalAvg = new GlobalAvg(1, 0);
    globalAvg.setChildren(new Operator[] { globalAgg });

    final Top1 topRevenue = new Top1(1);
    topRevenue.setChildren(new Operator[] { globalAvg });
    final CollectProducer sendToMaster = new CollectProducer(topRevenue, sendToMasterID, 0);

    final Map<Integer, SingleQueryPlanWithArgs> result = new HashMap<Integer, SingleQueryPlanWithArgs>();
    for (int worker : allWorkers) {
      result.put(worker, new SingleQueryPlanWithArgs(new RootOperator[] { spLocalScan, sendToMaster }));
    }

    return result;
  }

  @Override
  public SinkRoot getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect = new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    final Top1 topRevenue = new Top1(1);
    topRevenue.setChildren(new Operator[] { serverCollect });
    SinkRoot serverPlan = new SinkRoot(topRevenue);
    return serverPlan;
  }

  public static void main(String[] args) throws Exception {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(new JoinQueryPostgres().getMasterPlan(new int[] { 0, 1 }, null));
    oos.writeObject(new JoinQueryPostgres().getWorkerPlan(new int[] { 0, 1 }));
  }
}
