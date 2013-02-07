package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class MultithreadScanTest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1Worker1 = 50;

  public TupleBatchBuffer getResultInMemory(final TupleBatchBuffer table1, final Schema schema, final int numIteration) {
    // a brute force check

    final Iterator<List<Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    final boolean graph[][] = new boolean[MaxID][MaxID];
    final boolean cntgraph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      final List<Column<?>> output = tbs.next();
      final int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        final int fr = Integer.parseInt(output.get(0).get(i).toString());
        final int fe = Integer.parseInt(output.get(1).get(i).toString());
        graph[fr][fe] = cntgraph[fr][fe] = true;
      }
    }

    final boolean tmpgraph[][] = new boolean[MaxID][MaxID];
    int cnt = 0;
    while (true) {
      ++cnt;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (graph[j][i] && cntgraph[i][k]) {
              tmpgraph[j][k] = true;
            }
          }
        }
      }
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          cntgraph[i][j] = tmpgraph[i][j];
          tmpgraph[i][j] = false;
        }
      }
      if (cnt == numIteration - 1) {
        break;
      }
    }

    final TupleBatchBuffer result = new TupleBatchBuffer(schema);
    for (int i = 0; i < MaxID; ++i) {
      for (int j = 0; j < MaxID; ++j) {
        if (cntgraph[i][j]) {
          result.put(0, (long) i);
          result.put(1, (long) j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    LOGGER.debug("" + result.numTuples());
    return result;
  }

  @Test
  public void OneThreadTwoConnectionsTest() throws DbException, CatalogException, IOException {

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final ImmutableList<Type> joinTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> joinColumnNames = ImmutableList.of("follower1", "followee1", "follower2", "followee2");
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    final long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }

    final TupleBatchBuffer expectedTBB = getResultInMemory(tbl1Worker1, tableSchema, 2);
    final TupleBatchBuffer expectedTBBCopy = new TupleBatchBuffer(tableSchema);
    expectedTBBCopy.merge(expectedTBB);
    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    createTable(WORKER_ID[1], testtableKey, "follower long, followee long");
    // }
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
      insert(WORKER_ID[1], testtableKey, tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final LocalJoin localjoin = new LocalJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });
    final Project proj = new Project(new Integer[] { 0, 3 }, localjoin);
    final DupElim de = new DupElim(proj);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(de, serverReceiveID, MASTER_ID);

    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp });

    final long queryId = 1L;

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);

    expectedTBB.merge(expectedTBBCopy);
    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), TestUtils.tupleBatchToTupleBag(result));
  }

  @Test
  public void TwoThreadsTwoConnectionsSameDBFileTest() throws DbException, CatalogException, IOException {

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final ImmutableList<Type> joinTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> joinColumnNames = ImmutableList.of("follower1", "followee1", "follower2", "followee2");
    final Schema joinSchema = new Schema(joinTypes, joinColumnNames);

    final long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    final TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    final TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);

    final TupleBatchBuffer expectedTBB = getResultInMemory(table1, tableSchema, 2);
    final TupleBatchBuffer expectedTBBCopy = getResultInMemory(table1, tableSchema, 2);

    final RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    createTable(WORKER_ID[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
      insert(WORKER_ID[1], testtableKey, tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final LocalJoin localjoin1 = new LocalJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });
    final Project proj1 = new Project(new Integer[] { 0, 3 }, localjoin1);
    final DupElim de1 = new DupElim(proj1);
    final SQLiteQueryScan scan3 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan4 = new SQLiteQueryScan(null, "select * from " + testtableKey, tableSchema);
    final LocalJoin localjoin2 = new LocalJoin(joinSchema, scan3, scan4, new int[] { 1 }, new int[] { 0 });
    final Project proj2 = new Project(new Integer[] { 0, 3 }, localjoin2);
    final DupElim de2 = new DupElim(proj2);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition); // 2 workers
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column

    ExchangePairID arrayID1, arrayID2;
    arrayID1 = ExchangePairID.newID();
    arrayID2 = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(de1, arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleProducer sp2 = new ShuffleProducer(de2, arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), arrayID1, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc2 =
        new ShuffleConsumer(sp2.getSchema(), arrayID2, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final Merge merge = new Merge(tableSchema, sc1, sc2);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(merge, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp, sp1, sp2 });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp, sp1, sp2 });

    final long queryId = 0L;

    final CollectConsumer serverPlan =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    server.dispatchWorkerQueryPlans(queryId, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    TupleBatchBuffer result = server.startServerQuery(queryId, serverPlan);

    expectedTBB.merge(expectedTBBCopy);
    expectedTBB.merge(expectedTBBCopy);
    expectedTBB.merge(expectedTBBCopy);

    TestUtils.assertTupleBagEqual(TestUtils.tupleBatchToTupleBag(expectedTBB), TestUtils.tupleBatchToTupleBag(result));
  }
}
