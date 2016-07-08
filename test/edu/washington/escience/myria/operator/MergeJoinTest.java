package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map.Entry;

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

public class MergeJoinTest {

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
  public void testMergeJoinLarge() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = TestUtils.generateRandomTuples(12200, 12000, true);
    randomTuples[1] = TestUtils.generateRandomTuples(13200, 13000, true);

    // we need to rename the columns from the second tuples
    ImmutableList.Builder<String> sb = ImmutableList.builder();
    sb.add("id");
    sb.add("name");
    sb.add("id2");
    sb.add("name2");
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
}
