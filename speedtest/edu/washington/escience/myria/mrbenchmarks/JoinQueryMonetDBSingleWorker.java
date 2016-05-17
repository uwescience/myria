package edu.washington.escience.myria.mrbenchmarks;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class JoinQueryMonetDBSingleWorker implements QueryPlanGenerator, Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  final static ImmutableList<Type> scanTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> scanColumnNames =
      ImmutableList.of("sourceIPAddr", "sum(adRevenue)", "sum(pageRank)", "count(pageRank)");
  final static Schema scanSchema = new Schema(scanTypes, scanColumnNames);

  final static ImmutableList<Type> outputTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames =
      ImmutableList.of("sourceIPAddr", "totalAvenue", "avgPageRank");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final DbQueryScan localScan =
        new DbQueryScan( //
            "select sourceIPAddr, sum(adRevenue) as sumAdRevenue, sum(pageRank) as sumPageRank, count(pageRank) as countPageRank"
                + //
                "  from UserVisits, Rankings"
                + //
                "  where visitDate between date '2000-01-15' and date '2000-01-22'"
                + //
                "        and Rankings.pageURL=UserVisits.destinationURL"
                + //
                "  group by sourceIPAddr",
            scanSchema);

    GlobalAvg globalAvg = new GlobalAvg(2, 3);
    globalAvg.setChildren(new Operator[] {localScan});
    final Top1 topRevenue = new Top1(1);
    topRevenue.setChildren(new Operator[] {globalAvg});
    final CollectProducer sendToMaster = new CollectProducer(topRevenue, sendToMasterID, 0);

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
    final Top1 topRevenue = new Top1(1);
    topRevenue.setChildren(new Operator[] {serverCollect});
    SinkRoot serverPlan = new SinkRoot(topRevenue);
    return serverPlan;
  }

  public static void main(String[] args) throws Exception {
    final ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(inMemBuffer);
    oos.writeObject(new JoinQueryMonetDBSingleWorker().getMasterPlan(new int[] {0, 1}, null));
    oos.writeObject(new JoinQueryMonetDBSingleWorker().getWorkerPlan(new int[] {0, 1}));
  }
}
