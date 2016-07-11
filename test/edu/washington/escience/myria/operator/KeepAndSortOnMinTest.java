package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class KeepAndSortOnMinTest {
  @Test
  public void testKeepAndSortedOnMinValue() throws DbException {
    final int N = 52345;
    final int MaxID = 10000;
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("id", "value"));

    TupleBatchBuffer input = new TupleBatchBuffer(schema);
    final int[] ids = new int[N];
    for (int i = 0; i < N; ++i) {
      ids[i] = i;
    }
    long[] values1 = TestUtils.randomLong(0, MaxID, N);
    long[] values2 = TestUtils.randomLong(0, MaxID, N);
    long[] values3 = TestUtils.randomLong(0, MaxID, N);
    long[] minValue = new long[N];
    for (int i = 0; i < N; ++i) {
      input.putLong(0, ids[i]);
      input.putLong(1, values1[i]);
      input.putLong(0, ids[i]);
      input.putLong(1, values2[i]);
      input.putLong(0, ids[i]);
      input.putLong(1, values3[i]);
      minValue[i] = Math.min(values1[i], Math.min(values2[i], values3[i]));
    }

    TupleSource scan = new TupleSource(input);
    StreamingStateWrapper keepmin =
        new StreamingStateWrapper(scan, new KeepAndSortOnMinValue(new int[] {0}, new int[] {1}));
    keepmin.open(null);
    while (!keepmin.eos()) {
      keepmin.nextReady();
      long lastValue = -1;
      List<TupleBatch> result = keepmin.getStreamingState().exportState();
      for (TupleBatch tb : result) {
        for (int i = 0; i < tb.numTuples(); i++) {
          long value = tb.getLong(1, i);
          assertTrue(lastValue <= value);
          lastValue = value;
        }
      }
    }

    long lastValue = -1;
    double sum = 0;
    List<TupleBatch> result = keepmin.getStreamingState().exportState();
    for (TupleBatch tb : result) {
      for (int i = 0; i < tb.numTuples(); i++) {
        long value = tb.getLong(1, i);
        assertTrue(lastValue <= value);
        lastValue = value;
        assertTrue(minValue[(int) (tb.getLong(0, i))] == value);
      }
      sum += tb.numTuples();
    }
    keepmin.close();
    assertTrue(sum == N);
  }
}
