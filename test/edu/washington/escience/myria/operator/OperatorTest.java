package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.util.TestUtils;

public class OperatorTest {
  public TupleBatchBuffer generateRandomTuples(final int numTuples) {
    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(123));
    children[1] = new TupleSource(generateRandomTuples(42));
    children[2] = new TupleSource(generateRandomTuples(99));
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
  public void testCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = generateRandomTuples(123);
    randomTuples[1] = generateRandomTuples(42);

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb;
    HashSet<Long> idCollector = new HashSet<Long>();
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          idCollector.add(tb.getLong(0, i));
        }
      }
    }
    union.close();

    for (TupleBatchBuffer randomTuple : randomTuples) {
      for (TupleBatch tuples : randomTuple.getAll()) {
        for (int j = 0; j < tuples.numTuples(); j++) {
          assert idCollector.contains(tuples.getLong(0, j));
        }
      }
    }
  }
}
