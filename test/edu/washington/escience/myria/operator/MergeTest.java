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

public class MergeTest {

  @Test
  public void testMergeConstructorWithNull() throws DbException {
    TupleSource[] children = new TupleSource[1];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(10, 10, false));
    Merge merge = new Merge(null, null, null);
    merge.setChildren(children);
    merge.setSortedColumns(new int[] {0}, new boolean[] {true});
  }

  @Test
  public void testMergeCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(12300, 5000, true));
    children[1] = new TupleSource(TestUtils.generateRandomTuples(4200, 2000, true));
    children[2] = new TupleSource(TestUtils.generateRandomTuples(9900, 5000, true));
    NAryOperator merge = new Merge(children, new int[] {0}, new boolean[] {true});
    merge.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!merge.eos()) {
      tb = merge.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    merge.close();
    assertEquals(12300 + 4200 + 9900, count);
  }

  @Test
  public void testMergeTuplesSorted() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[3];
    randomTuples[0] = TestUtils.generateRandomTuples(52300, 5000, true);
    randomTuples[1] = TestUtils.generateRandomTuples(14200, 5000, true);
    randomTuples[2] = TestUtils.generateRandomTuples(29900, 5000, true);

    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);
    children[2] = new TupleSource(randomTuples[2]);

    NAryOperator merge = new Merge(children, new int[] {0, 1}, new boolean[] {true, true});
    merge.open(null);
    TupleBatch tb;
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();
    while (!merge.eos()) {
      tb = merge.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          entries.add(new SimpleEntry<Long, String>(tb.getLong(0, i), tb.getString(1, i)));
        }
      }
    }
    merge.close();

    assertEquals(52300 + 14200 + 29900, entries.size());

    Comparator<Entry<Long, String>> comparator = new TestUtils.EntryComparator();
    Entry<Long, String> previous = null;
    for (Entry<Long, String> entry : entries) {
      if (previous != null) {
        assertTrue(comparator.compare(previous, entry) <= 0);
      }
      previous = entry;
    }
  }
}
