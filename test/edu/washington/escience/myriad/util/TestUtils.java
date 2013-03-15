package edu.washington.escience.myriad.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;

import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.systemtest.SystemTestBase.Tuple;

public class TestUtils {

  public static void assertEqualsToStringBuilder(final StringBuilder errorMessageHolder, final String currentEM,
      final Object expected, final Object actual) {
    if (expected == null) {
      if (actual != null) {
        errorMessageHolder.append(currentEM);
        errorMessageHolder.append(": ");
        errorMessageHolder.append("expected: <null>");
        errorMessageHolder.append("but was: <");
        errorMessageHolder.append(actual);
        errorMessageHolder.append(">\n");
      }
    } else {
      if (!expected.equals(actual)) {
        errorMessageHolder.append(currentEM);
        errorMessageHolder.append(": ");
        errorMessageHolder.append("expected: <");
        errorMessageHolder.append(expected);
        errorMessageHolder.append('>');
        errorMessageHolder.append("but was: <");
        errorMessageHolder.append(actual);
        errorMessageHolder.append(">\n");
      }
    }
  }

  public static void assertTupleBagEqual(final HashMap<Tuple, Integer> expectedResult,
      final HashMap<Tuple, Integer> actualResult) {
    final StringBuilder errorMessageHolder = new StringBuilder();
    assertEqualsToStringBuilder(errorMessageHolder, "Number of unique tuples", expectedResult.size(), actualResult
        .size());
    final HashSet<Tuple> keySet = new HashSet<Tuple>();
    keySet.addAll(expectedResult.keySet());
    keySet.addAll(actualResult.keySet());
    for (final Tuple k : keySet) {
      Integer expected = expectedResult.get(k);
      Integer actual = actualResult.get(k);
      if (expected == null) {
        expected = 0;
      }
      if (actual == null) {
        actual = 0;
      }
      assertEqualsToStringBuilder(errorMessageHolder, "Tuple entry{" + k + "}", expected, actual);
    }
    if (errorMessageHolder.length() != 0) {
      Assert.fail(errorMessageHolder.toString());
    }
  }

  public static HashMap<Tuple, Integer> distinct(final TupleBatchBuffer content) {
    final Iterator<List<Column<?>>> it = content.getAllAsRawColumn().iterator();
    final HashMap<Tuple, Integer> expectedResults = new HashMap<Tuple, Integer>();
    while (it.hasNext()) {
      final List<Column<?>> columns = it.next();
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

  public static String intToString(final long v, final int length) {
    final StringBuilder sb = new StringBuilder("" + v);
    while (sb.length() < length) {
      sb.insert(0, "0");
    }
    return sb.toString();
  }

  public static HashMap<Tuple, Integer> mergeBags(final List<HashMap<Tuple, Integer>> bags) {
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    result.putAll(bags.get(0));
    for (int i = 1; i < bags.size(); i++) {
      for (final Map.Entry<Tuple, Integer> e : bags.get(i).entrySet()) {
        final Tuple t = e.getKey();
        final Integer occ = e.getValue();
        final Integer existingOcc = result.get(t);
        if (existingOcc == null) {
          result.put(t, occ);
        } else {
          result.put(t, occ + existingOcc);
        }
      }
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  public static HashMap<Tuple, Integer> naturalJoin(final TupleBatchBuffer child1, final TupleBatchBuffer child2,
      final int child1JoinColumn, final int child2JoinColumn) {

    /**
     * join key -> {tuple->num occur}
     * */
    final HashMap<Comparable, HashMap<Tuple, Integer>> child1Hash = new HashMap<Comparable, HashMap<Tuple, Integer>>();

    int numChild1Column = 0;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final List<List<Column<?>>> child1TBIt = child1.getAllAsRawColumn();
    for (final List<Column<?>> child1RawData : child1TBIt) {
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

    final Iterator<List<Column<?>>> child2TBIt = child2.getAllAsRawColumn().iterator();
    while (child2TBIt.hasNext()) {
      final List<Column<?>> child2Columns = child2TBIt.next();
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

  /***/
  public static String[] randomFixedLengthNumericString(final int min, final int max, final int size, final int length) {

    final String[] result = new String[size];
    final long[] intV = randomLong(min, max, size);

    for (int i = 0; i < size; i++) {
      result[i] = intToString(intV[i], length);
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

  public static HashMap<Tuple, Integer> tupleBatchToTupleBag(final TupleBatchBuffer tbb) {
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<List<Column<?>>> it = tbb.getAllAsRawColumn().iterator();

    while (it.hasNext()) {
      final List<Column<?>> columns = it.next();
      final int numColumn = columns.size();
      final int numRow = columns.get(0).size();
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
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<List<Column<?>>> it = tbb.getAllAsRawColumn().iterator();
    while (it.hasNext()) {
      final List<Column<?>> columns = it.next();
      final int numColumn = columns.size();
      final int numRow = columns.get(0).size();
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

}
