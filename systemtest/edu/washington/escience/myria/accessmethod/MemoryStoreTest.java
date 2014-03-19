package edu.washington.escience.myria.accessmethod;

import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.memorydb.MemoryStore;
import edu.washington.escience.myria.memorydb.MemoryStoreInfo;
import edu.washington.escience.myria.memorydb.ProtobufTable;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class MemoryStoreTest {

  private static final int NUM_TUPLES = 1000;
  private static Schema schema;
  private static TupleBatchBuffer data;

  /**
   * Setup what we need for the tests in this file.
   * 
   * @throws Exception if setUp fails.
   */
  @BeforeClass
  public static void setUp() throws Exception {

    /* Create the data needed for the tests in this file. */
    schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE));
    data = new TupleBatchBuffer(schema);
    /* Populate the TupleBatchBuffer. */
    final Random r = new Random();
    for (int i = 0; i < NUM_TUPLES; ++i) {
      data.putInt(0, i);
      data.putString(1, i + "th " + r.nextInt());
    }
  }

  @Test
  public void test() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    final RelationKey tuplesKey = RelationKey.of("test", "test", "my_tuples");

    final TupleSource source = new TupleSource(data);
    HashMap<String, Object> execEnv = new HashMap<String, Object>();
    MemoryStore mdb = new MemoryStore();
    execEnv.put(MyriaConstants.EXEC_ENV_VAR_MEMORY_STORE, mdb);
    final ImmutableMap<String, Object> execEnvVars = ImmutableMap.copyOf(execEnv);
    final MemoryStoreInfo connectionInfo = new MemoryStoreInfo(ProtobufTable.class);
    final DbInsert insert = new DbInsert(source, tuplesKey, connectionInfo);
    insert.setChildren(new Operator[] { source });
    insert.open(execEnvVars);
    while (!insert.eos()) {
      insert.nextReady();
    }
    insert.close();

    HashMap<Tuple, Integer> expected = TestUtils.tupleBatchToTupleBag(data);
    TupleBatchBuffer actual = new TupleBatchBuffer(schema);

    DbQueryScan scan = new DbQueryScan(connectionInfo, tuplesKey, schema);
    scan.open(execEnvVars);
    TupleBatch tb = null;
    while ((tb = scan.nextReady()) != null) {
      actual.appendTB(tb);
    }
    scan.close();

    TestUtils.assertTupleBagEqual(expected, TestUtils.tupleBatchToTupleBag(actual));
  }

  /**
   * Cleanup what we created.
   * 
   * @throws Exception if setUp fails.
   */
  @AfterClass
  public static void cleanUp() throws Exception {
  }

}
