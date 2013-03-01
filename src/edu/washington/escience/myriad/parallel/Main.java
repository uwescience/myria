package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

/**
 * Runs some simple tests.
 * 
 * @author dhalperi, slxu
 * 
 */
public final class Main {

  private static final int MASTER_ID = 0;
  private static int[] WORKER_ID;
  private static final int numPartition = 6;

  private static ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
  private static ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
  private static Schema tableSchema = new Schema(table1Types, table1ColumnNames);
  private static ImmutableList<Type> joinTypes = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE,
      Type.LONG_TYPE);
  private static ImmutableList<String> joinColumnNames = ImmutableList.of("follower", "followee", "follower",
      "followee");
  private static Schema joinSchema = new Schema(joinTypes, joinColumnNames);

  public static void iterativeSelfJoinTest() throws DbException, IOException {

    WORKER_ID = new int[numPartition];
    for (int i = 1; i <= numPartition; ++i) {
      WORKER_ID[i - 1] = i;
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from testtable", tableSchema);

    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    ShuffleProducer sp1 = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf1);
    ShuffleProducer sp2 = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf0);

    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1.getSchema(), arrayID1, WORKER_ID);
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2.getSchema(), arrayID2, WORKER_ID);
    final LocalJoin localjoin = new LocalJoin(joinSchema, sc1, sc2, new int[] { 1 }, new int[] { 0 });
    final Project proj = new Project(new int[] { 0, 3 }, localjoin);
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(proj, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    for (int i = 0; i < numPartition; ++i) {
      workerPlans.put(WORKER_ID[i], new Operator[] { sp1, sp2, cp });
    }

    final Long queryId = 9L;

    final CollectConsumer serverPlan = new CollectConsumer(tableSchema, serverReceiveID, WORKER_ID);
    serverInstance.dispatchWorkerQueryPlans(queryId, workerPlans);
    System.out.println("Query dispatched to the workers");
    serverInstance.startServerQuery(queryId, serverPlan);

  }

  public static void main(final String[] args) throws Exception {
    assert (args.length > 0);
    startMaster(args[0]);
    iterativeSelfJoinTest();
  }

  private static Server serverInstance;

  static Server startMaster(String catalogDir) {
    try {
      final String catalogFileName = FilenameUtils.concat(catalogDir, "master.catalog");
      serverInstance = new Server(catalogFileName);
      serverInstance.start();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    return serverInstance;
  }

  /** Inaccessible. */
  private Main() {
  }
}
