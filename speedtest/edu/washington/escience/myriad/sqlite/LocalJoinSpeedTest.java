package edu.washington.escience.myriad.sqlite;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.systemtest.SystemTestBase;

// 15s on my desktop
public class LocalJoinSpeedTest extends SystemTestBase {

  // the paths to the dataset on your local machine, copy them from /projects/db7/dataset/twitter/speedtest then change
  // the paths here
  private final static String[] srcPath = {
      "/media/jwang/myriad/test_worker1.db", "/media/jwang/myriad/test_worker2.db" };
  private final static String[] dstName = { "testtable0.db", "testtable0.db" };

  @Test
  public void localJoinSpeedTest() throws DbException, CatalogException, IOException {

    final Type[] table1Types = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] table1ColumnNames = new String[] { "follower", "followee" };
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final Type[] joinTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE };
    final String[] joinColumnNames = new String[] { "follower", "followee", "follower", "followee" };
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    final SQLiteQueryScan scan1 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("testtable0.db", "select * from testtable", tableSchema);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);

    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LocalJoin localjoin = new LocalJoin(joinSchema, sc1, sc2, new int[] { 1 }, new int[] { 0 });
    final Project proj = new Project(new Integer[] { 0, 3 }, localjoin);
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final ShuffleProducer sp0 = new ShuffleProducer(proj, arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleConsumer sc0 = new ShuffleConsumer(sp0, arrayID0, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final DupElim dupelim = new DupElim(sc0);
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp);
    workerPlans.put(WORKER_ID[1], cp);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    TupleBatchBuffer result = null;
    while ((result = Server.runningInstance.startServerQuery(0, serverPlan)) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
    assertTrue(result.numTuples() == 3361461);
  }

  @BeforeClass
  public static void loadSpecificTestData() {
    for (int i = 0; i < srcPath.length; ++i) {
      Path src = FileSystems.getDefault().getPath(srcPath[i]);
      Path dst =
          FileSystems.getDefault().getPath(workerTestBaseFolder + "/worker_" + (i + 1) + "/sqlite_dbs/" + dstName[i]);
      try {
        Files.copy(src, dst);
      } catch (Exception e) {
        throw new RuntimeException(
            "unable to copy file from "
                + srcPath[i]
                + " to "
                + dstName[i]
                + ". you can find the dataset at /projects/db7/dataset/twitter/speedtest, copy them to your local machine then point the srcPath to them");
      }
    }
  }
}
