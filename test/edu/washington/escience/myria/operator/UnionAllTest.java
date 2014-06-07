package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class UnionAllTest {

  @Test
  public void testUnionAllConstructorWithNull() throws DbException {
    TupleSource[] children = new TupleSource[1];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(10, 1000, false));
    UnionAll union = new UnionAll(null);
    union.setChildren(children);
  }

  @Test
  public void testUnionAllCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = TestUtils.generateRandomTuples(12300, 5000, false);
    randomTuples[1] = TestUtils.generateRandomTuples(4200, 2000, false);

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
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(TestUtils.generateRandomTuples(12300, 5000, false));
    children[1] = new TupleSource(TestUtils.generateRandomTuples(4200, 2000, false));
    children[2] = new TupleSource(TestUtils.generateRandomTuples(19900, 5000, false));
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
}
