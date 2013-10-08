package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;

import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.util.TestUtils;

public class OperatorTest {

  public class EntryComparator implements Comparator<Entry<Long, String>> {

    @Override
    public int compare(Entry<Long, String> o1, Entry<Long, String> o2) {
      int res = o1.getKey().compareTo(o2.getKey());
      if (res != 0) {
        return res;
      }
      return o1.getValue().compareTo(o2.getValue());
    }

  }

  /**
   * 
   * @param numTuples
   * @param sorted Generate sorted tuples, sorted by id
   * @return
   */
  public TupleBatchBuffer generateRandomTuples(final int numTuples, boolean sorted) {
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();

    final String[] names = TestUtils.randomFixedLengthNumericString(0, 5000, numTuples, 20);
    final long[] ids = TestUtils.randomLong(0, 2000, names.length);

    for (int i = 0; i < names.length; i++) {
      entries.add(new SimpleEntry<Long, String>(ids[i], names[i]));
    }

    Comparator<Entry<Long, String>> comparator = new EntryComparator();
    if (sorted) {
      Collections.sort(entries, comparator);
    }

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);

    for (Entry<Long, String> entry : entries) {
      tbb.put(0, entry.getKey());
      tbb.put(1, entry.getValue());
    }
    return tbb;
  }

  @Test
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(123, false));
    children[1] = new TupleSource(generateRandomTuples(42, false));
    children[2] = new TupleSource(generateRandomTuples(99, false));
    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    union.close();
    assertEquals(123 + 42 + 99, count);
  }

  @Test
  public void testUnionAllCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = generateRandomTuples(123, false);
    randomTuples[1] = generateRandomTuples(42, false);

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb;

    Multiset<Long> actualCounts = HashMultiset.create();
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          long index = tb.getLong(0, i);
          actualCounts.add(index);
        }
      }
    }
    union.close();

    Multiset<Long> expectedCounts = HashMultiset.create();
    for (TupleBatchBuffer randomTuple : randomTuples) {
      for (TupleBatch tuples : randomTuple.getAll()) {
        for (int j = 0; j < tuples.numTuples(); j++) {
          Long index = tuples.getLong(0, j);
          expectedCounts.add(index);
        }
      }
    }

    for (Multiset.Entry<Long> expectedEntry : expectedCounts.entrySet()) {
      assertEquals(expectedEntry.getCount(), actualCounts.count(expectedEntry.getElement()));
    }
  }

  @Test
  public void testMergeCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(12300, true));
    children[1] = new TupleSource(generateRandomTuples(4200, true));
    children[2] = new TupleSource(generateRandomTuples(9900, true));
    NAryOperator merge = new Merge(children, new int[] { 0 }, new boolean[] { true });
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
    randomTuples[0] = generateRandomTuples(52300, true);
    randomTuples[1] = generateRandomTuples(14200, true);
    randomTuples[2] = generateRandomTuples(29900, true);

    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);
    children[2] = new TupleSource(randomTuples[2]);

    NAryOperator merge = new Merge(children, new int[] { 0, 1 }, new boolean[] { true, true });
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

    Comparator<Entry<Long, String>> comparator = new EntryComparator();
    Entry<Long, String> previous = null;
    for (Entry<Long, String> entry : entries) {
      if (previous != null) {
        assertTrue(comparator.compare(previous, entry) <= 0);
      }
      previous = entry;
    }

  }
}
