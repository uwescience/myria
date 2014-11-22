package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;
import edu.washington.escience.myria.util.TestUtils;

public class LimitTest {
  @Test
  public void testSimplePrefix() throws DbException {
    final int total = 2 * TupleBatch.BATCH_SIZE + 2;
    final long limit = TupleBatch.BATCH_SIZE + 3;
    assertTrue(limit < total);
    TupleSource source = new TupleSource(TestUtils.range(total));
    Limit limiter = new Limit(limit, source);
    limiter.open(TestEnvVars.get());
    long count = 0;
    while (!limiter.eos()) {
      TupleBatch tb = limiter.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    limiter.close();
    assertEquals(limit, count);
  }
}
