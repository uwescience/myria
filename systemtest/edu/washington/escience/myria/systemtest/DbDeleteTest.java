/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * Test DbDelete Operator
 * 
 * This test performs a DbDelete under two different scenarios:
 * 
 * testDeleteRelationInCatalog(): This tests whether the Catalog deletes the relation after calling deleteDataset() from
 * the server
 * 
 * testDeleteNonExistentRelation(): This tests whether deleteDataset() is able to delete the tables from the underlying
 * database even if it does not exist in all workers expressed by the Catalog.
 * 
 */
public class DbDeleteTest extends SystemTestBase {
  /**
   * Source of tuples
   */
  TupleSource relationSource;

  /**
   * The relation to ingest
   */
  RelationKey relationKey;

  /**
   * Schema of the ingested relation
   */
  Schema relationSchema;

  /**
   * Tests if the relation has been deleted from the Catalog successfully.
   * 
   * @throws Exception
   */
  @Test
  public void testDeleteRelationInCatalog() throws Exception {
    ingestTestDataset();

    Set<Integer> workers = server.getWorkersForRelation(relationKey, null);
    assertTrue(workers.size() == workerIDs.length);

    JsonAPIUtils.deleteDataset("localhost", masterDaemonPort, relationKey.getUserName(), relationKey.getProgramName(),
        relationKey.getRelationName());

    workers = server.getWorkersForRelation(relationKey, null);
    assertTrue(workers == null);
  }

  /**
   * Tests if the relation has been deleted successfully from the underlying databases on all the workers even if a
   * worker does not contain the dataset to begin with.
   * 
   * @throws Exception
   */
  @Test
  public void testDeleteNonExistentRelation() throws Exception {
    ingestTestDataset();

    /* delete relation in one worker only */
    deleteTable(workerIDs[0], relationKey);

    assertFalse(existsTable(workerIDs[0], relationKey));
    assertTrue(existsTable(workerIDs[1], relationKey));

    JsonAPIUtils.deleteDataset("localhost", masterDaemonPort, relationKey.getUserName(), relationKey.getProgramName(),
        relationKey.getRelationName());

    assertFalse(existsTable(workerIDs[0], relationKey));
    assertFalse(existsTable(workerIDs[1], relationKey));
  }

  /**
   * Ingest a test dataset.
   */
  public void ingestTestDataset() throws Exception {
    DataSource relationSource = new FileSource(Paths.get("testdata", "filescan", "simple_two_col_int.txt").toString());
    relationKey = RelationKey.of("public", "adhoc", "testIngest");
    relationSchema = Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("x", "y"));
    JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(relationKey, relationSchema, relationSource, ' '));
  }
}
