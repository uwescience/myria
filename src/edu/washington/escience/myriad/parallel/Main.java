package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.DupElim;
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
  private static final int numIteration = 2;
  private static final int numPartition = 2;

  private static ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
  private static ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
  private static Schema tableSchema = new Schema(table1Types, table1ColumnNames);
  private static ImmutableList<Type> joinTypes = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE,
      Type.LONG_TYPE);
  private static ImmutableList<String> joinColumnNames = ImmutableList.of("follower", "followee", "follower",
      "followee");
  private static Schema joinSchema = new Schema(joinTypes, joinColumnNames);

  public static Operator getQueryPlan_full() throws DbException, IOException {

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);

    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column

    final ShuffleProducer[] sp0 = new ShuffleProducer[numIteration];
    final ShuffleProducer[] sp1 = new ShuffleProducer[numIteration];
    final ShuffleProducer[] sp2 = new ShuffleProducer[numIteration];
    final ShuffleConsumer[] sc0 = new ShuffleConsumer[numIteration];
    final ShuffleConsumer[] sc1 = new ShuffleConsumer[numIteration];
    final ShuffleConsumer[] sc2 = new ShuffleConsumer[numIteration];
    final LocalJoin[] localjoin = new LocalJoin[numIteration];
    final Project[] proj = new Project[numIteration];
    final DupElim[] dupelim = new DupElim[numIteration];
    final SQLiteQueryScan[] scan = new SQLiteQueryScan[numIteration];
    ExchangePairID arrayID1, arrayID2, arrayID0;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    sp1[0] = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf1);
    sp2[0] = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf0);

    for (int i = 1; i < numIteration; ++i) {
      sc1[i] = new ShuffleConsumer(sp1[i - 1], arrayID1, WORKER_ID);
      sc2[i] = new ShuffleConsumer(sp2[i - 1], arrayID2, WORKER_ID);
      localjoin[i] = new LocalJoin(joinSchema, sc1[i], sc2[i], new int[] { 1 }, new int[] { 0 });
      proj[i] = new Project(new Integer[] { 0, 3 }, localjoin[i]);
      arrayID0 = ExchangePairID.newID();
      sp0[i] = new ShuffleProducer(proj[i], arrayID0, WORKER_ID, pf0);
      sc0[i] = new ShuffleConsumer(sp0[i], arrayID0, WORKER_ID);
      dupelim[i] = new DupElim(sc0[i]);
      if (i == numIteration - 1) {
        break;
      }
      scan[i] = new SQLiteQueryScan("testtable" + (i + 1) + ".db", "select * from testtable", tableSchema);
      arrayID1 = ExchangePairID.newID();
      arrayID2 = ExchangePairID.newID();
      sp1[i] = new ShuffleProducer(scan[i], arrayID1, WORKER_ID, pf1);
      sp2[i] = new ShuffleProducer(dupelim[i], arrayID2, WORKER_ID, pf0);
    }
    return dupelim[numIteration - 1];
  }

  public static void iterativeSelfJoinTest(final String[] args) throws DbException, IOException {

    WORKER_ID = new int[numPartition];
    for (int i = 1; i <= numPartition; ++i) {
      WORKER_ID[i - 1] = i;
    }

    final Operator lastOne = getQueryPlan_full();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(lastOne, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    for (int i = 0; i < numPartition; ++i) {
      workerPlans.put(WORKER_ID[i], new Operator[] { cp });
    }
    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan = new CollectConsumer(tableSchema, serverReceiveID, WORKER_ID);
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    while (Server.runningInstance.startServerQuery(0, serverPlan) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

  }

  public static void main(final String[] args) throws Exception {
    // CatalogMaker.makeTwoNodeLocalParallelCatalog("/tmp/multitest");
    startMaster();
    iterativeSelfJoinTest(args);
  }

  static Server startMaster() {
    new Thread() {
      @Override
      public void run() {
        try {
          String catalogFileName = FilenameUtils.concat("/tmp/multitest", "master.catalog");
          Server.main(new String[] { catalogFileName });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }.start();
    return Server.runningInstance;
  }

  /** Inaccessible. */
  private Main() {
  }
}
