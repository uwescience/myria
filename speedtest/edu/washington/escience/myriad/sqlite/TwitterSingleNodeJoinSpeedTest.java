package edu.washington.escience.myriad.sqlite;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.DbQueryScan;

public class TwitterSingleNodeJoinSpeedTest {
  /**
   * The path to the dataset on your local machine. If not present, copy them from
   * /projects/db7/dataset/twitter/speedtest .
   */
  private final static String DATASET_PATH = "data_nocommit/speedtest/twitter/twitter_subset.db";

  /** Whether we were able to copy the data. */
  private static boolean successfulSetup = false;

  @BeforeClass
  public static void loadSpecificTestData() {
    final File file = new File(DATASET_PATH);
    if (!file.exists()) {
      throw new RuntimeException("Unable to read " + DATASET_PATH
          + ". Copy it from /projects/db7/dataset/twitter/speedtest .");
    }
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
    final DbQueryScan scan1 = new DbQueryScan("select * from twitter_subset", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan("select * from twitter_subset", tableSchema);

    // Join on SC1.followee=SC2.follower
    final LocalJoin localJoin =
        new LocalJoin(scan1, scan2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });

    /* Project down to only the two columns of interest: SC1.follower now transitively follows SC2.followee. */
    final Project proj = new Project(new int[] { 0, 3 }, localJoin);

    /* Now Dupelim */
    final DupElim dupelim = new DupElim(proj);

    dupelim.open(null);
    long result = 0;
    while (!dupelim.eos()) {
      final TupleBatch next = dupelim.nextReady();
      if (next != null) {
        result += next.numTuples();
      }
    }

    /* Make sure the count matches the known result. */
    assertTrue(result == 3361461);
  }

  @Test
  public void twitterSubsetProjectingJoinTest() throws DbException, CatalogException, IOException, InterruptedException {
    assertTrue(successfulSetup);

    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    /* Read the data from the file. */
    final DbQueryScan scan1 = new DbQueryScan("select * from twitter_subset", tableSchema);
    final DbQueryScan scan2 = new DbQueryScan("select * from twitter_subset", tableSchema);

    // Join on SC1.followee=SC2.follower
    final LocalJoin localProjJoin =
        new LocalJoin(scan1, scan2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    /* Now Dupelim */
    final DupElim dupelim = new DupElim(localProjJoin);

    dupelim.open(null);
    long result = 0;
    while (!dupelim.eos()) {
      final TupleBatch next = dupelim.nextReady();
      if (next != null) {
        result += next.numTuples();
      }
    }

    System.out.println(result + " tuples found.");

    /* Make sure the count matches the known result. */
    assertTrue(result == 3361461);
  }
}
