package edu.washington.escience.myria.sqlite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Applys;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.LocalFragmentResourceManager;
import edu.washington.escience.myria.storage.TupleBatch;

public class TwitterSingleNodeJoinSpeedTest {
  /**
   * The path to the dataset on your local machine. If not present, copy them from
   * /projects/db7/dataset/twitter/speedtest .
   */
  private final static String DATASET_PATH = "data_nocommit/speedtest/twitter/twitter_subset.db";

  /**
   * The ConnectionInfo (SQLiteInfo) object that tells Myria's AccessMethod operator where to find the database.
   */
  private final static ConnectionInfo connectionInfo = SQLiteInfo.of(DATASET_PATH);

  /**
   * The environment execution variables.
   */
  private final static ImmutableMap<String, Object> execEnvVars = ImmutableMap.<String, Object> of(
      MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER, new LocalFragmentResourceManager(null, null),
      MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, QueryExecutionMode.BLOCKING);

  /** Whether we were able to copy the data. */
  private static boolean successfulSetup = false;

  /** The name of the indexed twitter subset. */
  private final static RelationKey indexedSubset = RelationKey.of("Speedtest", "Twitter", "twitter_subset_with_index");

  @BeforeClass
  public static void loadSpecificTestData() throws DbException {
    final File file = new File(DATASET_PATH);
    if (!file.exists()) {
      throw new RuntimeException("Unable to read " + DATASET_PATH
          + ". Copy it from /projects/db7/dataset/twitter/speedtest .");
    }

    /* Create a version of the twitter subset that has an index. */
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    final DbQueryScan scan = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);
    final DbInsert insert =
        new DbInsert(scan, indexedSubset, connectionInfo, true, ImmutableList.of((List<IndexRef>) ImmutableList.of(
            IndexRef.of(0), IndexRef.of(1)), ImmutableList.of(IndexRef.of(1), IndexRef.of(0))));
    insert.open(execEnvVars);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.cleanup();
    insert.close();

    successfulSetup = true;
  }

  @Test
  public void twitterSubsetJoinTest() throws DbException, CatalogException, IOException, InterruptedException {
    assertTrue(successfulSetup);

    /* The Schema for the table we read from file. */
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);

    /* Join on SC1.followee=SC2.follower */
    final List<String> joinSchema = ImmutableList.of("follower", "joinL", "joinR", "followee");
    final SymmetricHashJoin join = new SymmetricHashJoin(joinSchema, scan1, scan2, new int[] { 1 }, new int[] { 0 });

    /* Select only the two columns of interest: SC1.follower now transitively follows SC2.followee. */
    final Apply colSelect = Applys.columnSelect(join, 0, 3);

    /* Now Dupelim */
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(colSelect, new DupElim());

    dupelim.open(execEnvVars);
    long result = 0;
    while (!dupelim.eos()) {
      final TupleBatch next = dupelim.nextReady();
      if (next != null) {
        result += next.numTuples();
      }
    }
    dupelim.close();

    /* Make sure the count matches the known result. */
    assertEquals(3361461, result);
  }

  @Test
  public void twitterSubsetColSelectJoinTest() throws DbException, CatalogException, IOException, InterruptedException {
    assertTrue(successfulSetup);

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);

    /* Join on SC1.followee=SC2.follower */
    final SymmetricHashJoin localColSelectJoin =
        new SymmetricHashJoin(scan1, scan2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now Dupelim */
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(localColSelectJoin, new DupElim());

    dupelim.open(execEnvVars);
    long result = 0;
    while (!dupelim.eos()) {
      final TupleBatch next = dupelim.nextReady();
      if (next != null) {
        result += next.numTuples();
      }
    }
    dupelim.close();

    /* Make sure the count matches the known result. */
    assertEquals(3361461, result);
  }

  @Test
  public void twitterSubsetColSelectJoinWithInsertTest() throws Exception {
    assertTrue(successfulSetup);

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan(connectionInfo, "select * from twitter_subset", tableSchema);

    /* Join on SC1.followee=SC2.follower */
    final SymmetricHashJoin localColSelectJoin =
        new SymmetricHashJoin(scan1, scan2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now Dupelim */
    final StreamingStateWrapper dupelim = new StreamingStateWrapper(localColSelectJoin, new DupElim());
    final RelationKey distinctJoinStored = RelationKey.of("Speedtest", "TwitterSingleNodeJoinSpeedTest", "TwitterJoin");
    /* .. and insert */
    final DbInsert insert = new DbInsert(dupelim, distinctJoinStored, connectionInfo, true);

    /* Run insert to completion. */
    insert.open(execEnvVars);
    while (!insert.eos()) {
      insert.nextReady();
    }

    /* Cleanup and close insert. */
    insert.cleanup();
    insert.close();

    /* Phase 2: Get the result out and verify it's what we expect. */
    final DbQueryScan scanResult = new DbQueryScan(connectionInfo, distinctJoinStored, tableSchema);
    scanResult.open(execEnvVars);
    long result = 0;
    while (!scanResult.eos()) {
      final TupleBatch next = scanResult.nextReady();
      if (next != null) {
        result += next.numTuples();
      }
    }
    scanResult.cleanup();
    scanResult.close();

    /* Make sure the count matches the known result. */
    assertEquals(3361461, result);
  }

  @Test
  public void twitterJoinInDatabaseTest() throws Exception {
    assertTrue(successfulSetup);

    final Schema resultSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("COUNT"));
    final String query =
        "SELECT COUNT(*) FROM (SELECT DISTINCT twitterL.follower,twitterR.followee FROM twitter_subset twitterL JOIN twitter_subset twitterR ON twitterL.followee=twitterR.follower)";
    final DbQueryScan scanResult = new DbQueryScan(connectionInfo, query, resultSchema);

    /* Run insert to completion. */
    scanResult.open(execEnvVars);
    TupleBatch tb = scanResult.nextReady();
    while (!scanResult.eos() && tb == null) {
      tb = scanResult.nextReady();
    }
    assertTrue(tb != null);
    scanResult.cleanup();
    scanResult.close();

    /* Check the result. */
    assertEquals(3361461, tb.getLong(0, 0));
  }

  @Test
  public void twitterJoinInDatabaseWithIndexTest() throws Exception {
    assertTrue(successfulSetup);

    final Schema resultSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("COUNT"));
    final String query =
        "SELECT COUNT(*) FROM (SELECT DISTINCT twitterL.follower,twitterR.followee FROM "
            + indexedSubset.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " twitterL JOIN "
            + indexedSubset.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)
            + " twitterR ON twitterL.followee=twitterR.follower)";
    final DbQueryScan scanResult = new DbQueryScan(connectionInfo, query, resultSchema);

    /* Run insert to completion. */
    scanResult.open(execEnvVars);
    TupleBatch tb = scanResult.nextReady();
    while (!scanResult.eos() && tb == null) {
      tb = scanResult.nextReady();
    }
    assertTrue(tb != null);
    scanResult.cleanup();
    scanResult.close();

    /* Check the result. */
    assertEquals(3361461, tb.getLong(0, 0));
  }
}
