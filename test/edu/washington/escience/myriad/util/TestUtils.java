package edu.washington.escience.myriad.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;

public class TestUtils {

  @SuppressWarnings("rawtypes")
  public static HashMap<Tuple, Integer> naturalJoin(final TupleBatchBuffer child1, final TupleBatchBuffer child2,
      final int child1JoinColumn, final int child2JoinColumn) {

    TupleBatch child1TB = null;

    /**
     * join key -> {tuple->num occur}
     * */
    final HashMap<Comparable, HashMap<Tuple, Integer>> child1Hash = new HashMap<Comparable, HashMap<Tuple, Integer>>();

    int numChild1Column = 0;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    Iterator<TupleBatch> child1TBIt = child1.getAll().iterator();
    while (child1TBIt.hasNext()) {
      child1TB = child1TBIt.next();
      final List<Column<?>> child1RawData = child1TB.outputRawData();
      final int numRow = child1RawData.get(0).size();
      final int numColumn = child1RawData.size();
      numChild1Column = numColumn;

      for (int i = 0; i < numRow; i++) {
        final Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, child1RawData.get(j).get(i));
        }
        final Object joinKey = t.get(child1JoinColumn);
        HashMap<Tuple, Integer> tupleOccur = child1Hash.get(joinKey);
        if (tupleOccur == null) {
          tupleOccur = new HashMap<Tuple, Integer>();
          tupleOccur.put(t, 1);
          child1Hash.put((Comparable<?>) joinKey, tupleOccur);
        } else {
          Integer occur = tupleOccur.get(t);
          if (occur == null) {
            occur = 0;
          }
          tupleOccur.put(t, occur + 1);
        }
      }
    }

    Iterator<TupleBatch> child2TBIt = child2.getAll().iterator();
    TupleBatch child2TB = null;
    while (child2TBIt.hasNext()) {
      child2TB = child2TBIt.next();
      final List<Column<?>> child2Columns = child2TB.outputRawData();
      final int numRow = child2Columns.get(0).size();
      final int numChild2Column = child2Columns.size();
      for (int i = 0; i < numRow; i++) {
        final Object joinKey = child2Columns.get(child2JoinColumn).get(i);
        final HashMap<Tuple, Integer> matchedTuples = child1Hash.get(joinKey);
        if (matchedTuples != null) {
          final Tuple child2Tuple = new Tuple(numChild2Column);

          for (int j = 0; j < numChild2Column; j++) {
            child2Tuple.set(j, child2Columns.get(j).get(i));
          }

          for (final Entry<Tuple, Integer> entry : matchedTuples.entrySet()) {
            final Tuple child1Tuple = entry.getKey();
            final int numChild1Occur = entry.getValue();

            final Tuple t = new Tuple(numChild1Column + numChild2Column);
            t.setAll(0, child1Tuple);
            t.setAll(numChild1Column, child2Tuple);
            final Integer occur = result.get(t);
            if (occur == null) {
              result.put(t, numChild1Occur);
            } else {
              result.put(t, occur + numChild1Occur);
            }
          }
        }
      }
    }
    return result;

  }

  public static HashMap<Tuple, Integer> distinct(final TupleBatchBuffer content) {
    final Iterator<TupleBatch> it = content.getAll().iterator();
    final HashMap<Tuple, Integer> expectedResults = new HashMap<Tuple, Integer>();
    while (it.hasNext()) {
      final TupleBatch tb = it.next();
      final List<Column<?>> columns = tb.outputRawData();
      final int numRow = columns.get(0).size();
      final int numColumn = columns.size();

      for (int i = 0; i < numRow; i++) {
        final Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, columns.get(j).get(i));
        }
        expectedResults.put(t, 1);
      }
    }
    return expectedResults;

  }

  public static void assertTupleBagEqual(HashMap<Tuple, Integer> expectedResult, HashMap<Tuple, Integer> actualResult) {
    Assert.assertEquals(expectedResult.size(), actualResult.size());
    for (Entry<Tuple, Integer> e : actualResult.entrySet()) {
      Assert.assertTrue(expectedResult.get(e.getKey()).equals(e.getValue()));
    }
  }

  public static HashMap<Tuple, Integer> mergeBags(List<HashMap<Tuple, Integer>> bags) {
    HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    result.putAll(bags.get(0));
    for (int i = 1; i < bags.size(); i++) {
      for (Map.Entry<Tuple, Integer> e : bags.get(i).entrySet()) {
        Tuple t = e.getKey();
        Integer occ = e.getValue();
        Integer existingOcc = result.get(t);
        if (existingOcc == null) {
          result.put(t, occ);
        } else {
          result.put(t, occ + existingOcc);
        }
      }
    }
    return result;
  }

  public static HashMap<Tuple, Integer> tupleBatchToTupleBag(final TupleBatchBuffer tbb) {
    TupleBatch tb = null;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final int numRow = tb.numTuples();
      final List<Column<?>> columns = tb.outputRawData();
      final int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        final Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, columns.get(column).get(row));
        }
        final Integer numOccur = result.get(t);
        if (numOccur == null) {
          result.put(t, new Integer(1));
        } else {
          result.put(t, numOccur + 1);
        }
      }
    }
    return result;
  }

  public static HashMap<Tuple, Integer> tupleBatchToTupleSet(final TupleBatchBuffer tbb) {
    TupleBatch tb = null;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<TupleBatch> it = tbb.getAll().iterator();
    while (it.hasNext()) {
      tb = it.next();
      final int numRow = tb.numTuples();
      final List<Column<?>> columns = tb.outputRawData();
      final int numColumn = columns.size();
      for (int row = 0; row < numRow; row++) {
        final Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, columns.get(column).get(row));
        }
        result.put(t, 1);
      }
    }
    return result;
  }

  public static long[] randomLong(final long min, final long max, final int size) {
    final long[] result = new long[size];
    final Random r = new Random();
    final long top = max - min + 1;
    for (int i = 0; i < size; i++) {
      result[i] = r.nextInt((int) top) + min;
    }
    return result;
  }

  /***/
  public static String[] randomFixedLengthNumericString(final int min, final int max, final int size, final int length) {

    final String[] result = new String[size];
    final long[] intV = randomLong(min, max, size);

    for (int i = 0; i < size; i++) {
      result[i] = intToString(intV[i], length);
    }
    return result;
  }

  public static String intToString(final long v, final int length) {
    final StringBuilder sb = new StringBuilder("" + v);
    while (sb.length() < length) {
      sb.insert(0, "0");
    }
    return sb.toString();
  }

}
