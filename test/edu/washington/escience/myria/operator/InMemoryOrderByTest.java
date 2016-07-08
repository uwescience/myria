package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class InMemoryOrderByTest {

  @Test
  public void testInMemoryOrderBy() throws DbException {
    TupleBatchBuffer randomTuples = TestUtils.generateRandomTuples(52300, 5000, false);

    TupleSource child = new TupleSource(randomTuples);

    InMemoryOrderBy order =
        new InMemoryOrderBy(child, new int[] {0, 1}, new boolean[] {true, true});
    order.open(null);
    TupleBatch tb;
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();
    while (!order.eos()) {
      tb = order.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          entries.add(new SimpleEntry<Long, String>(tb.getLong(0, i), tb.getString(1, i)));
        }
      }
    }
    order.close();

    assertEquals(52300, entries.size());

    Comparator<Entry<Long, String>> comparator = new TestUtils.EntryComparator();
    Entry<Long, String> previous = null;
    for (Entry<Long, String> entry : entries) {
      if (previous != null) {
        assertTrue(comparator.compare(previous, entry) <= 0);
      }
      previous = entry;
    }
  }

  @Test
  public void testInMemoryOrderBy2() throws DbException {
    // we had a bug where ordering by certain subsets of the columns caused index out of bound exceptions. in other
    // cases only the results were wrong.
    TupleBatchBuffer randomTuples = TestUtils.generateRandomTuples(52300, 5000, false);

    TupleSource child = new TupleSource(randomTuples);

    InMemoryOrderBy order = new InMemoryOrderBy(child, new int[] {1}, new boolean[] {false});
    order.open(null);
    TupleBatch tb;
    final ArrayList<String> entries = new ArrayList<String>();
    while (!order.eos()) {
      tb = order.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          entries.add(tb.getString(1, i));
        }
      }
    }
    order.close();

    assertEquals(52300, entries.size());

    String previous = null;
    for (String entry : entries) {
      if (previous != null) {
        assertTrue(previous.compareTo(entry) >= 0);
      }
      previous = entry;
    }
  }
}
