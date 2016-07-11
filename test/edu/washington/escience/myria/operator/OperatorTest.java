package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class OperatorTest {

  public class EntryComparator implements Comparator<Entry<Long, String>> {

    @Override
    public int compare(final Entry<Long, String> o1, final Entry<Long, String> o2) {
      int res = o1.getKey().compareTo(o2.getKey());
      if (res != 0) {
        return res;
      }
      return o1.getValue().compareTo(o2.getValue());
    }
  }

  /**
   * @param numTuples how many tuples in output
   * @param sampleSize how many different values should be created at random (around numTuples/sampleSize duplicates)
   * @param sorted Generate sorted tuples, sorted by id
   * @return
   */
  public TupleBatchBuffer generateRandomTuples(
      final int numTuples, final int sampleSize, final boolean sorted) {
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();

    final long[] ids = TestUtils.randomLong(0, sampleSize, numTuples);
    final String[] names = TestUtils.randomFixedLengthNumericString(0, sampleSize, numTuples, 20);

    for (int i = 0; i < numTuples; i++) {
      entries.add(new SimpleEntry<Long, String>(ids[i], names[i]));
    }

    Comparator<Entry<Long, String>> comparator = new EntryComparator();
    if (sorted) {
      Collections.sort(entries, comparator);
    }

    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);

    for (Entry<Long, String> entry : entries) {
      tbb.putLong(0, entry.getKey());
      tbb.putString(1, entry.getValue());
    }
    return tbb;
  }

  @Test
  public void testUnionAllConstructorWithNull() throws DbException {
    TupleSource[] children = new TupleSource[1];
    children[0] = new TupleSource(generateRandomTuples(10, 1000, false));
    UnionAll union = new UnionAll(null);
    union.setChildren(children);
  }

  @Test
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(12300, 5000, false));
    children[1] = new TupleSource(generateRandomTuples(4200, 2000, false));
    children[2] = new TupleSource(generateRandomTuples(19900, 5000, false));
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
    assertEquals(12300 + 4200 + 19900, count);
  }

  @Test
  public void testUnionAllCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = generateRandomTuples(12300, 5000, false);
    randomTuples[1] = generateRandomTuples(4200, 2000, false);

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
  public void testMergeConstructorWithNull() throws DbException {
    TupleSource[] children = new TupleSource[1];
    children[0] = new TupleSource(generateRandomTuples(10, 10, false));
    Merge merge = new Merge(null, null, null);
    merge.setChildren(children);
    merge.setSortedColumns(new int[] {0}, new boolean[] {true});
  }

  @Test
  public void testMergeCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(12300, 5000, true));
    children[1] = new TupleSource(generateRandomTuples(4200, 2000, true));
    children[2] = new TupleSource(generateRandomTuples(9900, 5000, true));
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
    randomTuples[0] = generateRandomTuples(52300, 5000, true);
    randomTuples[1] = generateRandomTuples(14200, 5000, true);
    randomTuples[2] = generateRandomTuples(29900, 5000, true);

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

    Comparator<Entry<Long, String>> comparator = new EntryComparator();
    Entry<Long, String> previous = null;
    for (Entry<Long, String> entry : entries) {
      if (previous != null) {
        assertTrue(comparator.compare(previous, entry) <= 0);
      }
      previous = entry;
    }
  }

  @Test
  public void testOrderedDupElim() throws DbException {
    TupleBatchBuffer randomTuples = generateRandomTuples(52300, 5000, true);
    TupleSource child = new TupleSource(randomTuples);
    OrderedDupElim dupElim = new OrderedDupElim(child);
    int count = 0;

    /* Count the dupelim */
    dupElim.open(null);
    while (!dupElim.eos()) {
      TupleBatch tb = dupElim.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    dupElim.close();

    /* Count the real answer */
    Map<Long, Set<String>> map = new HashMap<Long, Set<String>>();
    for (TupleBatch tuples : randomTuples.getAll()) {
      for (int i = 0; i < tuples.numTuples(); ++i) {
        Set<String> set = map.get(tuples.getLong(0, i));
        if (set == null) {
          set = new HashSet<String>();
          map.put(tuples.getLong(0, i), set);
        }
        set.add(tuples.getString(1, i));
      }
    }
    int realCount = 0;
    for (Set<String> set : map.values()) {
      realCount += set.size();
    }

    assertEquals(count, realCount);
  }

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

  @Test
  public void testMergeJoin() throws DbException {
    final Schema leftSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    TupleBatchBuffer leftTbb = new TupleBatchBuffer(leftSchema);

    {
      long[] ids = new long[] {0, 2, 2, 2, 3, 5, 6, 8, 8, 10};
      String[] names = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

      for (int i = 0; i < ids.length; i++) {
        leftTbb.putLong(0, ids[i]);
        leftTbb.putString(1, names[i]);
      }
    }

    final Schema rightSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id2", "name2"));

    TupleBatchBuffer rightTbb = new TupleBatchBuffer(rightSchema);

    {
      long[] ids = new long[] {1, 2, 2, 4, 8, 8, 10};
      String[] names = new String[] {"a", "b", "c", "d", "e", "f", "g"};

      for (int i = 0; i < ids.length; i++) {
        rightTbb.putLong(0, ids[i]);
        rightTbb.putString(1, names[i]);
      }
    }

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(leftTbb);
    children[1] = new TupleSource(rightTbb);

    BinaryOperator join =
        new MergeJoin(children[0], children[1], new int[] {0}, new int[] {0}, new boolean[] {true});
    join.open(null);
    TupleBatch tb;
    final ArrayList<TupleBatch> batches = new ArrayList<TupleBatch>();
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.add(tb);
      }
    }
    join.close();

    assertEquals(1, batches.size());
    assertEquals(11, batches.get(0).numTuples());
  }

  @Test
  public void testMergeJoinLarge() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = generateRandomTuples(12200, 12000, true);
    randomTuples[1] = generateRandomTuples(13200, 13000, true);

    // we need to rename the columns from the second tuples
    ImmutableList.Builder<String> sb = ImmutableList.builder();
    sb.add("id").add("name");
    sb.add("id2").add("name2");
    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    BinaryOperator join =
        new MergeJoin(
            sb.build(),
            children[0],
            children[1],
            new int[] {0},
            new int[] {0},
            new boolean[] {true});
    join.open(null);
    TupleBatch tb;
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          entries.add(new SimpleEntry<Long, String>(tb.getLong(0, i), tb.getString(1, i)));
        }
      }
    }

    // output should be sorted by join keys
    Entry<Long, String> previous = null;
    for (Entry<Long, String> entry : entries) {
      if (previous != null) {
        assertTrue(previous.getKey() <= entry.getKey());
      }
      previous = entry;
    }

    join.close();
  }

  @Test
  public void testMergeJoinCross() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("id", "value"));
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = new TupleBatchBuffer(schema);
    randomTuples[1] = new TupleBatchBuffer(schema);
    for (int i = 0; i < 5; i++) {
      randomTuples[0].putLong(0, 42);
      randomTuples[0].putLong(1, i);
      randomTuples[1].putLong(0, 42);
      randomTuples[1].putLong(1, 100 + i);
    }

    // we need to rename the columns from the second tuples
    ImmutableList.Builder<String> sb = ImmutableList.builder();
    sb.addAll(schema.getColumnNames());
    sb.add("id2");
    sb.add("value2");
    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    BinaryOperator join =
        new MergeJoin(
            sb.build(),
            children[0],
            children[1],
            new int[] {0},
            new int[] {0},
            new boolean[] {true});
    join.open(null);
    TupleBatch tb = null;
    int count = 0;
    Multiset<Long> left = HashMultiset.create();
    Multiset<Long> right = HashMultiset.create();
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        count += tb.numTuples();
        for (int i = 0; i < tb.numTuples(); i++) {
          left.add(tb.getLong(1, i));
          right.add(tb.getLong(3, i));
        }
      }
    }

    for (long i = 0; i < 5; i++) {
      assertEquals(5, left.count(i));
      assertEquals(5, right.count(i + 100));
    }

    assertEquals(25, count);

    join.close();
  }

  @Test
  public void testInMemoryOrderBy() throws DbException {
    TupleBatchBuffer randomTuples = generateRandomTuples(52300, 5000, false);

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

    Comparator<Entry<Long, String>> comparator = new EntryComparator();
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
    TupleBatchBuffer randomTuples = generateRandomTuples(52300, 5000, false);

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

  @Test
  public void testOrderByAndMergeJoin() throws DbException {
    final Schema leftSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    TupleBatchBuffer leftTbb = new TupleBatchBuffer(leftSchema);

    {
      long[] ids = new long[] {2, 3, 5, 6, 8, 8, 10, 0, 2, 2};
      String[] names = new String[] {"d", "e", "f", "g", "h", "i", "j", "a", "b", "c"};

      for (int i = 0; i < ids.length; i++) {
        leftTbb.putLong(0, ids[i]);
        leftTbb.putString(1, names[i]);
      }
    }

    final Schema rightSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id2", "name2"));

    TupleBatchBuffer rightTbb = new TupleBatchBuffer(rightSchema);

    {
      long[] ids = new long[] {1, 2, 2, 4, 8, 8, 10};
      String[] names = new String[] {"a", "b", "c", "d", "e", "f", "g"};

      for (int i = 0; i < ids.length; i++) {
        rightTbb.putLong(0, ids[i]);
        rightTbb.putString(1, names[i]);
      }
    }

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(leftTbb);
    children[1] = new TupleSource(rightTbb);

    InMemoryOrderBy sort0 = new InMemoryOrderBy(children[0], new int[] {0}, new boolean[] {false});
    InMemoryOrderBy sort1 = new InMemoryOrderBy(children[1], new int[] {0}, new boolean[] {false});

    BinaryOperator join =
        new MergeJoin(sort0, sort1, new int[] {0}, new int[] {0}, new boolean[] {false});
    join.open(null);
    TupleBatch tb;
    final ArrayList<TupleBatch> batches = new ArrayList<TupleBatch>();
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.add(tb);
      }
    }
    join.close();

    assertEquals(1, batches.size());
    assertEquals(11, batches.get(0).numTuples());
  }

  @Test
  public void testMergeJoinOnMultipleKeys() throws DbException {
    final Schema leftSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));
    TupleBatchBuffer leftTbb = new TupleBatchBuffer(leftSchema);

    {
      long[] ids = new long[] {0, 2, 2, 2, 3, 5, 6, 8, 8, 10};
      String[] names = new String[] {"c", "c", "c", "b", "b", "b", "b", "a", "a", "a"};

      for (int i = 0; i < ids.length; i++) {
        leftTbb.putLong(0, ids[i]);
        leftTbb.putString(1, names[i]);
      }
    }

    final Schema rightSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id2", "name2"));

    TupleBatchBuffer rightTbb = new TupleBatchBuffer(rightSchema);

    {
      long[] ids = new long[] {1, 2, 2, 4, 8, 8, 10, 11};
      String[] names = new String[] {"d", "d", "c", "c", "a", "a", "a", "a"};

      for (int i = 0; i < ids.length; i++) {
        rightTbb.putLong(0, ids[i]);
        rightTbb.putString(1, names[i]);
      }
    }

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(leftTbb);
    children[1] = new TupleSource(rightTbb);

    BinaryOperator join =
        new MergeJoin(
            children[0],
            children[1],
            new int[] {0, 1},
            new int[] {0, 1},
            new boolean[] {true, false});
    join.open(null);
    TupleBatch tb;
    final ArrayList<TupleBatch> batches = new ArrayList<TupleBatch>();
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.add(tb);
      }
    }
    join.close();

    assertEquals(1, batches.size());
    assertEquals(7, batches.get(0).numTuples());
  }
}
