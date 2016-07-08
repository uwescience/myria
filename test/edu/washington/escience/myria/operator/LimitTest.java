package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;
import edu.washington.escience.myria.util.TestUtils;

public class LimitTest {

  @Test
  public void testWithinBatchSizeLimit() throws DbException {
    final int total = TupleBatch.BATCH_SIZE;
    final long limit = 100;
    assertTrue(limit < total);
    TupleSource source = new TupleSource(TestUtils.range(total));
    Limit limiter = new Limit(limit, source);
    limiter.open(TestEnvVars.get());
    TupleBatch tb = limiter.nextReady();
    assertEquals(limit, tb.numTuples());
    // reached limit, limiter gets eos and next call to nextReady() returns null
    tb = limiter.nextReady();
    assertNull(tb);
    assertTrue(limiter.eos());
    limiter.close();
  }

  @Test
  public void testLimitZero() throws DbException {
    final int total = 2 * TupleBatch.BATCH_SIZE + 2;
    final long limit = 0;
    assertTrue(limit < total);
    TupleSource source = new TupleSource(TestUtils.range(total));
    Limit limiter = new Limit(limit, source);
    limiter.open(TestEnvVars.get());
    TupleBatch tb = limiter.nextReady();
    assertNull(tb);
    assertTrue(limiter.eos());
    limiter.close();
  }

  @Test
  public void testLimitNumTuples() throws DbException {
    final int total = 100;
    final long limit = total;
    TupleBatchBuffer tbb1 = TestUtils.range((int) limit);
    TupleBatchBuffer tbb2 = TestUtils.range((int) limit);
    List<TupleBatch> sourceList = ImmutableList.of(tbb1.popAny(), tbb2.popAny());
    TupleSource source = new TupleSource(sourceList);
    Limit limiter = new Limit(limit, source);
    limiter.open(TestEnvVars.get());
    TupleBatch tb = limiter.nextReady();
    assertEquals(limit, tb.numTuples());
    // reached limit, limiter gets eos and next call to nextReady() returns null
    tb = limiter.nextReady();
    assertNull(tb);
    assertTrue(limiter.eos());
    limiter.close();
  }

  @Test
  public void testSimplePrefix() throws DbException {
    final int total = 2 * TupleBatch.BATCH_SIZE + 2;
    final long limit = TupleBatch.BATCH_SIZE + 3;
    assertTrue(limit < total);
    TupleSource source = new TupleSource(TestUtils.range(total));
    Limit limiter = new Limit(limit, source);
    limiter.open(TestEnvVars.get());
    long count = 0;
    int numIteration = 0;
    while (!limiter.eos()) {
      TupleBatch tb = limiter.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
      numIteration++;
    }
    assertEquals(limit, count);
    assertEquals(2, numIteration);
    limiter.close();
  }
}
