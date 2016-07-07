package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SetGlobal;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class QueryGlobalTest extends SystemTestBase {

  @Test
  public void createQueryGlobal() throws Exception {
    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "key");
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    tuples.putLong(0, 5432L);
    final BatchTupleSource source = new BatchTupleSource(tuples);
    SetGlobal global = new SetGlobal(source, "myglobal", server);

    Query q = server.submitQueryPlan(global, new HashMap<Integer, RootOperator[]>()).get();
    assertEquals(q.getGlobal("myglobal"), 5432L);
  }

  @Test(expected = ExecutionException.class)
  public void badQueryGlobalTwoColumns() throws Exception {
    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "key", Type.STRING_TYPE, "value");
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    tuples.putLong(0, 5432L);
    tuples.putString(1, "hi");
    final BatchTupleSource source = new BatchTupleSource(tuples);
    SetGlobal global = new SetGlobal(source, "myglobal", server);

    server.submitQueryPlan(global, new HashMap<Integer, RootOperator[]>()).get();
  }

  @Test(expected = ExecutionException.class)
  public void badQueryGlobalTwoRows() throws Exception {
    final Schema schema = Schema.ofFields(Type.LONG_TYPE, "key");
    final TupleBatchBuffer tuples = new TupleBatchBuffer(schema);
    tuples.putLong(0, 5432L);
    tuples.putLong(0, 1234L);
    final BatchTupleSource source = new BatchTupleSource(tuples);
    SetGlobal global = new SetGlobal(source, "myglobal", server);

    server.submitQueryPlan(global, new HashMap<Integer, RootOperator[]>()).get();
  }
}
