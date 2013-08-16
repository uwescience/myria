package edu.washington.escience.myria.sqlite;

import static org.junit.Assert.assertTrue;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.LocalJoin;
import edu.washington.escience.myria.operator.Project;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.ShuffleConsumer;
import edu.washington.escience.myria.parallel.ShuffleProducer;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.systemtest.SystemTestBase;

// 15s on Jingjing's desktop
// about 44s on DanH's laptop
public class TwitterJoinSpeedTest extends SystemTestBase {

  // the paths to the dataset on your local machine, copy them from /projects/db7/dataset/twitter/speedtest then change
  // the paths here
  private final static String[] srcPath = {
      "data_nocommit/speedtest/twitter/test_worker1.db", "data_nocommit/speedtest/twitter/test_worker2.db" };
  private final static String[] dstName = { "testtable0.db", "testtable0.db" };

  /* Whether we were able to copy the data. */
  private static boolean successfulSetup = false;

  @BeforeClass
  public static void loadSpecificTestData() {
    for (int i = 0; i < srcPath.length; ++i) {
      final Path src = FileSystems.getDefault().getPath(srcPath[i]);
      final Path dst =
          FileSystems.getDefault().getPath(workerTestBaseFolder + "/worker_" + (i + 1) + "/sqlite_dbs/" + dstName[i]);
      try {
        Files.copy(src, dst);
      } catch (final Exception e) {
        throw new RuntimeException("unable to copy files from " + srcPath[i] + " to " + dstName[i]
            + ". you can find the dataset at /projects/db7/dataset/twitter/speedtest.");
      }
    }
    successfulSetup = true;
  }

  @Test
  public void twitterSubsetCountProjectingJoinTest() throws Exception {
    assertTrue(successfulSetup);

    /* The Schema for the table we read from file. */
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan("select * from testtable", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan("select * from testtable", tableSchema);

    /*
     * One worker partitions on the follower, the other worker partitions on the followee. In this way, we can do a
     * self-join at the local node.
     */
    final int numPartition = 2;
    // PF0 : follower (field 0 of the tuple)
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp2.getSchema(), arrayID2, WORKER_ID);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp1.getSchema(), arrayID1, WORKER_ID);
    // Join on SC1.followee=SC2.follower
    final LocalJoin localProjJoin =
        new LocalJoin(sc1, sc2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final ShuffleProducer sp0 = new ShuffleProducer(localProjJoin, arrayID0, WORKER_ID, pf0);
    final ShuffleConsumer sc0 = new ShuffleConsumer(sp0.getSchema(), arrayID0, WORKER_ID);
    final DupElim dupelim = new DupElim(sc0);
    final Aggregate count = new Aggregate(dupelim, new int[] { 0 }, new int[] { Aggregator.AGG_OP_COUNT });

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(count, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final Schema collectSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("COUNT"));
    final CollectConsumer collectCounts = new CollectConsumer(collectSchema, serverReceiveID, WORKER_ID);
    Aggregate sumCount = new Aggregate(collectCounts, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, sumCount);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    /* Make sure the count matches the known result. */
    assertTrue(receivedTupleBatches.take().getLong(0, 0) == 3361461);

  }

  @Test
  public void twitterSubsetJoinTest() throws Exception {
    assertTrue(successfulSetup);

    /* The Schema for the table we read from file. */
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan("select * from testtable", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan("select * from testtable", tableSchema);

    /*
     * One worker partitions on the follower, the other worker partitions on the followee. In this way, we can do a
     * self-join at the local node.
     */
    final int numPartition = 2;
    // PF0 : follower (field 0 of the tuple)
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp2.getSchema(), arrayID2, WORKER_ID);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp1.getSchema(), arrayID1, WORKER_ID);
    // Join on SC1.followee=SC2.follower
    final LocalJoin localjoin =
        new LocalJoin(sc1, sc2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Project down to only the two columns of interest: SC1.follower now transitively follows SC2.followee. */
    final Project proj = new Project(new int[] { 0, 3 }, localjoin);
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final ShuffleProducer sp0 = new ShuffleProducer(proj, arrayID0, WORKER_ID, pf0);
    final ShuffleConsumer sc0 = new ShuffleConsumer(sp0.getSchema(), arrayID0, WORKER_ID);
    final DupElim dupelim = new DupElim(sc0);

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final CollectConsumer collect = new CollectConsumer(cp.getSchema(), serverReceiveID, WORKER_ID);
    final SinkRoot serverPlan = new SinkRoot(collect);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    /* Make sure the count matches the known result. */
    assertTrue(serverPlan.getCount() == 3361461);

  }

  @Test
  public void twitterSubsetProjectingJoinTest() throws Exception {
    assertTrue(successfulSetup);

    /* The Schema for the table we read from file. */
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan("select * from testtable", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan("select * from testtable", tableSchema);

    /*
     * One worker partitions on the follower, the other worker partitions on the followee. In this way, we can do a
     * self-join at the local node.
     */
    final int numPartition = 2;
    // PF0 : follower (field 0 of the tuple)
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, arrayID1, WORKER_ID, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final ShuffleProducer sp2 = new ShuffleProducer(scan2, arrayID2, WORKER_ID, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp2.getSchema(), arrayID2, WORKER_ID);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp1.getSchema(), arrayID1, WORKER_ID);
    // Join on SC1.followee=SC2.follower
    final LocalJoin localProjJoin =
        new LocalJoin(sc1, sc2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final ShuffleProducer sp0 = new ShuffleProducer(localProjJoin, arrayID0, WORKER_ID, pf0);
    final ShuffleConsumer sc0 = new ShuffleConsumer(sp0.getSchema(), arrayID0, WORKER_ID);
    final DupElim dupelim = new DupElim(sc0);

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final CollectConsumer collect = new CollectConsumer(cp.getSchema(), serverReceiveID, WORKER_ID);
    final SinkRoot serverPlan = new SinkRoot(collect);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    /* Make sure the count matches the known result. */
    assertTrue(serverPlan.getCount() == 3361461);

  }
}
