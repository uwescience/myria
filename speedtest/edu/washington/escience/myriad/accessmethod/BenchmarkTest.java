/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.TupleSource;

/**
 * @author valmeida
 * 
 */
public class BenchmarkTest {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BenchmarkTest.class);

  /**
   * Test name.
   */
  private static final String BENCHMARKTEST_NAME = "dbmsBenchmark";

  /**
   * The hostname for the tests. betelgeuse has all databases installed and running. Please check that property before
   * any change.
   * 
   */
  private static final String BENCHMARKTEST_HOSTNAME = "betelgeuse";

  /* Test data */
  private TupleBatchBuffer buffer;
  private Schema schema;
  private RelationKey relationKey;
  private final static int NUM_TUPLES = 5 * TupleBatch.BATCH_SIZE + 1;
  private List<ConnectionInfo> connections = null;

  @Before
  public void init() {
    if (schema == null) {
      schema = Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("i1", "i2"));
      relationKey = RelationKey.of("test", "test", "big");
    }
    Random generator = new Random();
    if (buffer == null || buffer.numTuples() < NUM_TUPLES) {
      buffer = new TupleBatchBuffer(schema);
      for (int i = 0; i < NUM_TUPLES; ++i) {
        buffer.put(0, generator.nextInt());
        buffer.put(1, generator.nextInt());
      }
    }
    connections = new ArrayList<ConnectionInfo>();

    Path tempFilePath;
    try {
      tempFilePath = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_systemtests");
      /* The SQLite connection */
      String jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_SQLITE, BENCHMARKTEST_HOSTNAME, BENCHMARKTEST_NAME,
              tempFilePath.toFile().getAbsolutePath(), "0");
      connections.add(ConnectionInfo.of("sqlite", jsonConnInfo));

      /* The SQLite connection */
      jsonConnInfo =
          ConnectionInfo.toJson(MyriaConstants.STORAGE_SYSTEM_MONETDB, BENCHMARKTEST_HOSTNAME, BENCHMARKTEST_NAME,
              tempFilePath.toFile().getAbsolutePath(), "0");
      connections.add(ConnectionInfo.of("monetdb", jsonConnInfo));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testBenchmark() throws Exception {

    for (ConnectionInfo conn : connections) {
      /* Insert the NUM_TUPLES tuples */
      TupleSource source = new TupleSource(buffer);
      DbInsert insert = new DbInsert(source, relationKey, conn);
      insert.open(null);
      while (!insert.eos()) {
        insert.nextReady();
      }
      insert.close();

      /* Count them and make sure we got the right count. */
      DbQueryScan count =
          new DbQueryScan(conn, "SELECT COUNT(*) FROM " + relationKey.toString(conn.getDbms()), Schema.of(ImmutableList
              .of(Type.LONG_TYPE), ImmutableList.of("count")));
      count.open(null);

      TupleBatch result = count.nextReady();
      assertTrue(result != null);
      assertTrue(result.getLong(0, 0) == NUM_TUPLES);
      result = count.nextReady();
      assertTrue(result == null);
      assertTrue(count.eos());
      count.close();
    }
  }
}
