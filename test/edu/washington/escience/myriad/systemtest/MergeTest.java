package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.util.TestUtils;

public class MergeTest extends SystemTestBase {

  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1 = 73;
  private final int numTbl2 = 82;

  @Test
  public void mergeTest() throws Exception {

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    final long[] tbl1ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    final long[] tbl1ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl1);
    final long[] tbl2ID1 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final long[] tbl2ID2 = TestUtils.randomLong(1, MaxID - 1, numTbl2);
    final TupleBatchBuffer tbl1 = new TupleBatchBuffer(tableSchema);
    final TupleBatchBuffer tbl2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1; i++) {
      tbl1.put(0, tbl1ID1[i]);
      tbl1.put(1, tbl1ID2[i]);
    }
    for (int i = 0; i < numTbl2; i++) {
      tbl2.put(0, tbl2ID1[i]);
      tbl2.put(1, tbl2ID2[i]);
    }

    final RelationKey testtable0Key = RelationKey.of("test", "test", "testtable0");
    final RelationKey testtable1Key = RelationKey.of("test", "test", "testtable1");

    createTable(WORKER_ID[0], testtable0Key, "follower long, followee long");
    createTable(WORKER_ID[0], testtable1Key, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1.popAny()) != null) {
      insert(WORKER_ID[0], testtable0Key, tableSchema, tb);
    }
    while ((tb = tbl2.popAny()) != null) {
      insert(WORKER_ID[0], testtable1Key, tableSchema, tb);
    }

    final SQLiteQueryScan scan1 = new SQLiteQueryScan(testtable0Key, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan(testtable1Key, tableSchema);
    final Merge merge = new Merge(new Operator[] { scan1, scan2 });
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(merge, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp });

    final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0] });

    final SinkRoot serverPlan = new SinkRoot(serverCollect);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    assertTrue(serverPlan.getCount() == (numTbl1 + numTbl2));

  }
}
