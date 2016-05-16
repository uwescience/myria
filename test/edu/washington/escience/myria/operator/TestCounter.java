package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.Objects;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;

public class TestCounter {

  @Test
  public void test() throws DbException {
    long numTuples = 5 * TupleBatch.BATCH_SIZE + 3;
    String columnName = "Count";

    /* Plan: numTuples tuples, followed by a Counter. */
    final TupleRangeSource source = new TupleRangeSource(numTuples, Type.STRING_TYPE);
    final Counter counter = new Counter(source, columnName);

    /* Open and sanity check the counter. */
    counter.open(null);
    Objects.requireNonNull(counter.getSchema(), "Even after opening, Counter has null Schema.");
    Schema expected = Schema.appendColumn(source.getSchema(), Type.LONG_TYPE, columnName);
    assertEquals(expected, counter.getSchema());

    /* Accumulate the counter values. */
    Set<Long> set = Sets.newHashSet();
    long sum = 0;
    while (!counter.eos()) {
      TupleBatch tb = counter.nextReady();
      if (tb == null) {
        continue;
      }
      for (int i = 0; i < tb.numTuples(); ++i) {
        set.add(tb.getLong(1, i));
        sum += tb.getLong(1, i);
      }
    }
    counter.close();

    /* Ensure that all counter values are unique and they have the correct sum. */
    assertEquals(numTuples, set.size());
    assertEquals(numTuples * (numTuples - 1) / 2, sum);
  }
}
