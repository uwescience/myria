package edu.washington.escience.myria.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.JdbcInfo;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class SelectQueryPostgres implements QueryPlanGenerator {

  /**
   *
   */
  private static final long serialVersionUID = 6019962778188598959L;
  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.INT_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("pageURL", "pageRank");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  /* Connection information */
  public static final String host = "localhost";
  public static final int port = 5432;
  public static final String user = "ubuntu";
  public static final String password = "ubuntu";
  public static final String dbms = "postgresql";
  public static final String databaseName = "mrbenchmarks";
  public static final String jdbcDriverName = "org.postgresql.Driver";
  public static final JdbcInfo jdbcInfo =
      JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, password);

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    // SELECT pageURL, pageRank FROM Rankings WHERE pageRank > X;
    DbQueryScan selectPageRank =
        new DbQueryScan(
            "select pageURL, pageRank from Rankings where pageRank > 10 ", outputSchema);
    // final SQLiteQueryScan selectPageRank =
    // new SQLiteQueryScan("select pageURL, pageRank from Rankings where pageRank > 10 ", outputSchema);

    final CollectProducer sendToMaster = new CollectProducer(selectPageRank, sendToMasterID, 0);

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
