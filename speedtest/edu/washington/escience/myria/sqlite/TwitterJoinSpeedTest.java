package edu.washington.escience.myria.sqlite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Applys;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.systemtest.SystemTestBase;
import edu.washington.escience.myria.util.DeploymentUtils;

// 15s on Jingjing's desktop
// about 44s on DanH's laptop
public class TwitterJoinSpeedTest extends SystemTestBase {

  // the paths to the dataset on your local machine, copy them from /projects/db7/dataset/twitter/speedtest then change
  // the paths here
  private final static String[] srcPath = {
      "data_nocommit/speedtest/twitter/test_worker1.db", "data_nocommit/speedtest/twitter/test_worker2.db" };

  /* Whether we were able to copy the data. */
  private static boolean successfulSetup = false;

  @Override
  public void before() {
    /* Load specific test data. */
    for (int i = 0; i < srcPath.length; ++i) {
      final Path src = FileSystems.getDefault().getPath(srcPath[i]);
      final Path dst = Paths.get(DeploymentUtils.getPathToWorkerDir(workingDir, workerIDs[i]), "data.db");
      try {
        Files.copy(src, dst);
      } catch (final Exception e) {
        throw new RuntimeException("unable to copy files from " + src.toAbsolutePath() + " to " + dst.toAbsolutePath()
            + ". you can find the dataset at /projects/db7/dataset/twitter/speedtest.", e);
      }
    }
    successfulSetup = true;
  }

  @Test
  public void twitterSubsetCountColSelectJoinTest() throws Exception {
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
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final GenericShuffleProducer sp1 = new GenericShuffleProducer(scan1, arrayID1, workerIDs, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final GenericShuffleProducer sp2 = new GenericShuffleProducer(scan2, arrayID2, workerIDs, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final GenericShuffleConsumer sc1 = new GenericShuffleConsumer(sp2.getSchema(), arrayID2, workerIDs);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final GenericShuffleConsumer sc2 = new GenericShuffleConsumer(sp1.getSchema(), arrayID1, workerIDs);
    // Join on SC1.followee=SC2.follower
    final SymmetricHashJoin localProjJoin =
        new SymmetricHashJoin(sc1, sc2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final GenericShuffleProducer sp0 = new GenericShuffleProducer(localProjJoin, arrayID0, workerIDs, pf0);
    final GenericShuffleConsumer sc0 = new GenericShuffleConsumer(sp0.getSchema(), arrayID0, workerIDs);
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(sc0, new DupElim());
    final Aggregate count = new Aggregate(dupelim, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(count, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final Schema collectSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("COUNT"));
    final CollectConsumer collectCounts = new CollectConsumer(collectSchema, serverReceiveID, workerIDs);
    Aggregate sumCount = new Aggregate(collectCounts, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, sumCount);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    /* Make sure the count matches the known result. */
    assertEquals(3361461, receivedTupleBatches.take().getLong(0, 0));

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
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final GenericShuffleProducer sp1 = new GenericShuffleProducer(scan1, arrayID1, workerIDs, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final GenericShuffleProducer sp2 = new GenericShuffleProducer(scan2, arrayID2, workerIDs, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final GenericShuffleConsumer sc1 = new GenericShuffleConsumer(sp2.getSchema(), arrayID2, workerIDs);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final GenericShuffleConsumer sc2 = new GenericShuffleConsumer(sp1.getSchema(), arrayID1, workerIDs);
    // Join on SC1.followee=SC2.follower
    final List<String> joinSchema = ImmutableList.of("follower", "joinL", "joinR", "followee");
    final SymmetricHashJoin localjoin = new SymmetricHashJoin(joinSchema, sc1, sc2, new int[] { 1 }, new int[] { 0 });
    /* Select only the two columns of interest: SC1.follower now transitively follows SC2.followee. */
    final Apply proj = Applys.columnSelect(localjoin, 0, 3);
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final GenericShuffleProducer sp0 = new GenericShuffleProducer(proj, arrayID0, workerIDs, pf0);
    final GenericShuffleConsumer sc0 = new GenericShuffleConsumer(sp0.getSchema(), arrayID0, workerIDs);
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(sc0, new DupElim());

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final CollectConsumer collect = new CollectConsumer(cp.getSchema(), serverReceiveID, workerIDs);
    final SinkRoot serverPlan = new SinkRoot(collect);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    /* Make sure the count matches the known result. */
    assertEquals(3361461, serverPlan.getCount());

  }

  @Test
  public void twitterSubsetColSelectJoinTest() throws Exception {
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
    final PartitionFunction pf0 = new SingleFieldHashPartitionFunction(numPartition, 0);
    final ExchangePairID arrayID1 = ExchangePairID.newID();
    final GenericShuffleProducer sp1 = new GenericShuffleProducer(scan1, arrayID1, workerIDs, pf0);
    // PF1 : followee (field 1 of the tuple)
    final PartitionFunction pf1 = new SingleFieldHashPartitionFunction(numPartition, 1);
    final ExchangePairID arrayID2 = ExchangePairID.newID();
    final GenericShuffleProducer sp2 = new GenericShuffleProducer(scan2, arrayID2, workerIDs, pf1);

    /* Each worker receives both partitions, then joins on them. */
    // SC1: receive based on followee (PF1, so SP2 and arrayID2)
    final GenericShuffleConsumer sc1 = new GenericShuffleConsumer(sp2.getSchema(), arrayID2, workerIDs);
    // SC2: receive based on follower (PF0, so SP1 and arrayID1)
    final GenericShuffleConsumer sc2 = new GenericShuffleConsumer(sp1.getSchema(), arrayID1, workerIDs);
    // Join on SC1.followee=SC2.follower
    final SymmetricHashJoin localProjJoin =
        new SymmetricHashJoin(sc1, sc2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now reshuffle the results to partition based on the new followee, so that we can dupelim. */
    final ExchangePairID arrayID0 = ExchangePairID.newID();
    final GenericShuffleProducer sp0 = new GenericShuffleProducer(localProjJoin, arrayID0, workerIDs, pf0);
    final GenericShuffleConsumer sc0 = new GenericShuffleConsumer(sp0.getSchema(), arrayID0, workerIDs);
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(sc0, new DupElim());

    /* Finally, send (CollectProduce) all the results to the master. */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(dupelim, serverReceiveID, MASTER_ID);

    /* Send the worker plans, rooted by CP, to the workers. Note that the plans are identical. */
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp, sp1, sp2, sp0 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp, sp1, sp2, sp0 });

    /* The server plan. Basically, collect and count tuples. */
    final CollectConsumer collect = new CollectConsumer(cp.getSchema(), serverReceiveID, workerIDs);
    final SinkRoot serverPlan = new SinkRoot(collect);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    /* Make sure the count matches the known result. */
    assertEquals(3361461, serverPlan.getCount());

  }
}
