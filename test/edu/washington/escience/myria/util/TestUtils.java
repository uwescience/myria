package edu.washington.escience.myria.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.column.Column;

public final class TestUtils {

  private static Random random = null;

  private synchronized static Random getRandom() {
    if (random == null) {
      random = new Random();
    }
    return random;
  }

  public static void setSeed(long seed) {
    getRandom().setSeed(seed);
  }

  public static void resetRandom() {
    random = null;
  }

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

  public static HashMap<Tuple, Integer> groupByAvgLongColumn(final TupleBatchBuffer source, final int groupByColumn,
      final int aggColumn) {
    final List<List<Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, Long> sum = new HashMap<Object, Long>();
    final HashMap<Object, Integer> count = new HashMap<Object, Integer>();
    for (final List<Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).get(i);
        final Long aggValue = (Long) rawData.get(aggColumn).get(i);
        Long currentSum = sum.get(groupByValue);
        if (currentSum == null) {
          currentSum = 0L;
          count.put(groupByValue, 1);
        } else {
          count.put(groupByValue, count.get(groupByValue) + 1);
        }
        sum.put(groupByValue, currentSum + aggValue);
      }
    }
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();

    for (final Map.Entry<Object, Long> e : sum.entrySet()) {
      final Object gValue = e.getKey();
      final Long sumV = e.getValue();
      final Tuple t = new Tuple(2);
      t.set(0, (Comparable<?>) gValue);
      t.set(1, sumV * 1.0 / count.get(gValue));
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<Tuple, Integer> groupByCount(final TupleBatchBuffer source, final int groupByColumn) {
    final List<List<Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, Long> count = new HashMap<Object, Long>();
    for (final List<Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).get(i);
        Long currentCount = count.get(groupByValue);
        if (currentCount == null) {
          currentCount = 0L;
        }
        count.put(groupByValue, ++currentCount);
      }
    }
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();

    for (final Map.Entry<Object, Long> e : count.entrySet()) {
      final Object gValue = e.getKey();
      final Long countV = e.getValue();
      final Tuple t = new Tuple(2);
      t.set(0, (Comparable<?>) gValue);
      t.set(1, countV);
      result.put(t, 1);
    }
    return result;
  }

  public static <T extends Comparable<T>> HashMap<Tuple, Integer> groupByMax(final TupleBatchBuffer source,
      final int groupByColumn, final int aggColumn) {
    final List<List<Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, T> max = new HashMap<Object, T>();
    for (final List<Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).get(i);
        @SuppressWarnings("unchecked")
        final T aggValue = (T) rawData.get(aggColumn).get(i);
        final T currentMax = max.get(groupByValue);
        if (currentMax == null) {
          max.put(groupByValue, aggValue);
        } else if (aggValue.compareTo(currentMax) > 0) {
          max.put(groupByValue, aggValue);
        }
      }
    }
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();

    for (final Map.Entry<Object, T> e : max.entrySet()) {
      final Object gValue = e.getKey();
      final T maxV = e.getValue();
      final Tuple t = new Tuple(2);
      t.set(0, (Comparable<?>) gValue);
      t.set(1, maxV);
      result.put(t, 1);
    }
    return result;
  }

  public static <T extends Comparable<T>> HashMap<Tuple, Integer> groupByMin(final TupleBatchBuffer source,
      final int groupByColumn, final int aggColumn) {
    final List<List<Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, T> min = new HashMap<Object, T>();
    for (final List<Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).get(i);
        @SuppressWarnings("unchecked")
        final T aggValue = (T) rawData.get(aggColumn).get(i);
        final T currentMin = min.get(groupByValue);
        if (currentMin == null) {
          min.put(groupByValue, aggValue);
        } else if (aggValue.compareTo(currentMin) < 0) {
          min.put(groupByValue, aggValue);
        }
      }
    }
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();

    for (final Map.Entry<Object, T> e : min.entrySet()) {
      final Object gValue = e.getKey();
      final T minV = e.getValue();
      final Tuple t = new Tuple(2);
      t.set(0, (Comparable<?>) gValue);
      t.set(1, minV);
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<Tuple, Integer> groupBySumLongColumn(final TupleBatchBuffer source, final int groupByColumn,
      final int aggColumn) {
    final List<List<Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, Long> sum = new HashMap<Object, Long>();
    for (final List<Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).get(i);
        final Long aggValue = (Long) rawData.get(aggColumn).get(i);
        Long currentSum = sum.get(groupByValue);
        if (currentSum == null) {
          currentSum = 0L;
        }
        sum.put(groupByValue, currentSum + aggValue);
      }
    }
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();

    for (final Map.Entry<Object, Long> e : sum.entrySet()) {
      final Object gValue = e.getKey();
      final Long sumV = e.getValue();
      final Tuple t = new Tuple(2);
      t.set(0, (Comparable<?>) gValue);
      t.set(1, sumV);
      result.put(t, 1);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> T max(final TupleBatchBuffer tbb, final int column) {
    final List<List<Column<?>>> tbs = tbb.getAllAsRawColumn();
    T max = (T) tbs.get(0).get(column).get(0);
    for (final List<Column<?>> tb : tbs) {
      final int numTuples = tb.get(0).size();
      final Column<?> c = tb.get(column);
      for (int i = 0; i < numTuples; i++) {
        final T current = (T) c.get(i);
        if (max.compareTo(current) < 0) {
          max = current;
        }
      }
    }
    return max;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> T min(final TupleBatchBuffer tbb, final int column) {
    final List<List<Column<?>>> tbs = tbb.getAllAsRawColumn();
    T min = (T) tbs.get(0).get(column).get(0);
    for (final List<Column<?>> tb : tbs) {
      final int numTuples = tb.get(0).size();
      final Column<?> c = tb.get(column);
      for (int i = 0; i < numTuples; i++) {
        final T current = (T) c.get(i);
        if (min.compareTo(current) > 0) {
          min = current;
        }
      }
    }
    return min;
  }

  public static long sumLong(final TupleBatchBuffer tbb, final int column) {
    final List<List<Column<?>>> tbs = tbb.getAllAsRawColumn();
    long sum = 0;
    for (final List<Column<?>> tb : tbs) {
      final int numTuples = tb.get(0).size();
      final Column<?> c = tb.get(column);
      for (int i = 0; i < numTuples; i++) {
        final Long current = (Long) c.get(i);
        sum += current;
      }
    }
    return sum;
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

  public static int[] randomInt(final int min, final int max, final int size) {
    final int[] result = new int[size];
    final Random r = new Random();
    final int top = max - min + 1;
    for (int i = 0; i < size; i++) {
      result[i] = r.nextInt(top) + min;
    }
    return result;
  }

  public static long[] randomLong(final long min, final long max, final int size) {
    final long[] result = new long[size];
    final long top = max - min + 1;
    for (int i = 0; i < size; i++) {
      result[i] = getRandom().nextInt((int) top) + min;
    }
    return result;
  }

  public static float[] randomFloat(final float min, final float max, final int size) {
    final float[] result = new float[size];
    final float range = max - min;
    for (int i = 0; i < size; i++) {
      result[i] = getRandom().nextFloat() * range + min;
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

  public static ImmutableList.Builder<Number> generateListBuilderWithElement(long element) {
    ImmutableList.Builder<Number> sourceListBuilder = ImmutableList.builder();
    sourceListBuilder.add(element);
    return sourceListBuilder;
  }

}
