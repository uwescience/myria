package edu.washington.escience.myria.util;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Assume;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.failures.InitFailureInjector;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public final class TestUtils {

  public static class EntryComparator implements Comparator<Entry<Long, String>> {

    @Override
    public int compare(Entry<Long, String> o1, Entry<Long, String> o2) {
      int res = o1.getKey().compareTo(o2.getKey());
      if (res != 0) {
        return res;
      }
      return o1.getValue().compareTo(o2.getValue());
    }
  }

  private static Random random = null;

  /**
   * See http://docs.travis-ci.com/user/ci-environment/#Environment-variables
   *
   * @return <code>true</code> if the system is currently in a Travis CI build.
   */
  public static boolean inTravis() {
    String travis = System.getenv("TRAVIS");
    return (travis != null) && travis.equals("true");
  }

  /**
   * Only run this test in Travis.
   */
  public static void requireTravis() {
    Assume.assumeTrue(inTravis());
  }

  /**
   * Skip this test if in Travis.
   */
  public static void skipIfInTravis() {
    Assume.assumeFalse(inTravis());
  }

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

  public static void assertEqualsToStringBuilder(
      final StringBuilder errorMessageHolder,
      final String currentEM,
      final Object expected,
      final Object actual) {
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

  public static void assertTupleBagEqual(
      final HashMap<Tuple, Integer> expectedResult, final HashMap<Tuple, Integer> actualResult) {
    final StringBuilder errorMessageHolder = new StringBuilder();
    assertEqualsToStringBuilder(
        errorMessageHolder, "Number of unique tuples", expectedResult.size(), actualResult.size());
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
  public static HashMap<Tuple, Integer> naturalJoin(
      final TupleBatchBuffer child1,
      final TupleBatchBuffer child2,
      final int child1JoinColumn,
      final int child2JoinColumn) {

    /**
     * join key -> {tuple->num occur}
     * */
    final HashMap<Comparable, HashMap<Tuple, Integer>> child1Hash =
        new HashMap<Comparable, HashMap<Tuple, Integer>>();

    int numChild1Column = 0;
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final List<List<? extends Column<?>>> child1TBIt = child1.getAllAsRawColumn();
    for (final List<? extends Column<?>> child1RawData : child1TBIt) {
      final int numRow = child1RawData.get(0).size();
      final int numColumn = child1RawData.size();
      numChild1Column = numColumn;

      for (int i = 0; i < numRow; i++) {
        final Tuple t = new Tuple(numColumn);
        for (int j = 0; j < numColumn; j++) {
          t.set(j, child1RawData.get(j).getObject(i));
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

    final Iterator<List<? extends Column<?>>> child2TBIt = child2.getAllAsRawColumn().iterator();
    while (child2TBIt.hasNext()) {
      final List<? extends Column<?>> child2Columns = child2TBIt.next();
      final int numRow = child2Columns.get(0).size();
      final int numChild2Column = child2Columns.size();
      for (int i = 0; i < numRow; i++) {
        final Object joinKey = child2Columns.get(child2JoinColumn).getObject(i);
        final HashMap<Tuple, Integer> matchedTuples = child1Hash.get(joinKey);
        if (matchedTuples != null) {
          final Tuple child2Tuple = new Tuple(numChild2Column);

          for (int j = 0; j < numChild2Column; j++) {
            child2Tuple.set(j, child2Columns.get(j).getObject(i));
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

  public static HashMap<Tuple, Integer> groupByAvgLongColumn(
      final TupleBatchBuffer source, final int groupByColumn, final int aggColumn) {
    final List<List<? extends Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, Long> sum = new HashMap<Object, Long>();
    final HashMap<Object, Integer> count = new HashMap<Object, Integer>();
    for (final List<? extends Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).getObject(i);
        final Long aggValue = (Long) rawData.get(aggColumn).getObject(i);
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

  public static <T extends Comparable<T>> HashMap<Tuple, Integer> groupByMax(
      final TupleBatchBuffer source, final int groupByColumn, final int aggColumn) {
    final List<List<? extends Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, T> max = new HashMap<Object, T>();
    for (final List<? extends Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).getObject(i);
        @SuppressWarnings("unchecked")
        final T aggValue = (T) rawData.get(aggColumn).getObject(i);
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

  public static <T extends Comparable<T>> HashMap<Tuple, Integer> groupByMin(
      final TupleBatchBuffer source, final int groupByColumn, final int aggColumn) {
    final List<List<? extends Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, T> min = new HashMap<Object, T>();
    for (final List<? extends Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).getObject(i);
        @SuppressWarnings("unchecked")
        final T aggValue = (T) rawData.get(aggColumn).getObject(i);
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

  public static HashMap<Tuple, Integer> groupBySumLongColumn(
      final TupleBatchBuffer source, final int groupByColumn, final int aggColumn) {
    final List<List<? extends Column<?>>> tbs = source.getAllAsRawColumn();
    final HashMap<Object, Long> sum = new HashMap<Object, Long>();
    for (final List<? extends Column<?>> rawData : tbs) {
      final int numTuples = rawData.get(0).size();
      for (int i = 0; i < numTuples; i++) {
        final Object groupByValue = rawData.get(groupByColumn).getObject(i);
        final Long aggValue = (Long) rawData.get(aggColumn).getObject(i);
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

  /***/
  public static String[] randomFixedLengthNumericString(
      final int min, final int max, final int size, final int length) {

    final String[] result = new String[size];
    final long[] intV = randomLong(min, max, size);

    for (int i = 0; i < size; i++) {
      result[i] = intToString(intV[i], length);
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

  public static HashMap<Tuple, Integer> tupleBatchToTupleBag(final TupleBatchBuffer tbb) {
    final HashMap<Tuple, Integer> result = new HashMap<Tuple, Integer>();
    final Iterator<List<? extends Column<?>>> it = tbb.getAllAsRawColumn().iterator();

    while (it.hasNext()) {
      final List<? extends Column<?>> columns = it.next();
      final int numColumn = columns.size();
      final int numRow = columns.get(0).size();
      for (int row = 0; row < numRow; row++) {
        final Tuple t = new Tuple(numColumn);
        for (int column = 0; column < numColumn; column++) {
          t.set(column, columns.get(column).getObject(row));
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

  /**
   * @param numTuples how many tuples in output
   * @param sampleSize how many different values should be created at random (around numTuples/sampleSize duplicates)
   * @param sorted Generate sorted tuples, sorted by id
   * @return
   */
  public static TupleBatchBuffer generateRandomTuples(
      final int numTuples, final int sampleSize, boolean sorted) {
    final ArrayList<Entry<Long, String>> entries = new ArrayList<Entry<Long, String>>();

    final long[] ids = randomLong(0, sampleSize, numTuples);
    final String[] names = randomFixedLengthNumericString(0, sampleSize, numTuples, 20);

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

  /**
   * Construct a SubQuery that will insert the given tuples (starting on the master) on the specified workers using the
   * specified relation key and partition function.
   *
   * @param masterSource the source of tuples, from the master.
   * @param dest the name of the relation into which tuples will be inserted (using overwrite!).
   * @param pf how tuples will be partitioned on the cluster.
   * @param workers the set of workers on which the data will be stored.
   * @return a SubQuery that will insert the given tuples (starting on the master) on the specified workers using the
   *         specified relation key and partition function.
   */
  public static final SubQuery insertRelation(
      @Nonnull final Operator masterSource,
      @Nonnull final RelationKey dest,
      @Nonnull PartitionFunction pf,
      @Nonnull Set<Integer> workers) {
    return insertRelation(
        masterSource,
        dest,
        pf,
        ArrayUtils.toPrimitive(workers.toArray(new Integer[workers.size()])));
  }

  /**
   * Construct a SubQuery that will insert the given tuples (starting on the master) on the specified workers using the
   * specified relation key and partition function.
   *
   * @param masterSource the source of tuples, from the master.
   * @param dest the name of the relation into which tuples will be inserted (using overwrite!).
   * @param pf how tuples will be partitioned on the cluster.
   * @param workers the set of workers on which the data will be stored.
   * @return a SubQuery that will insert the given tuples (starting on the master) on the specified workers using the
   *         specified relation key and partition function.
   */
  public static final SubQuery insertRelation(
      @Nonnull final Operator masterSource,
      @Nonnull final RelationKey dest,
      @Nonnull PartitionFunction pf,
      @Nonnull int[] workers) {
    final ExchangePairID id = ExchangePairID.newID();
    /* Master plan */
    GenericShuffleProducer sp = new GenericShuffleProducer(masterSource, id, workers, pf);
    SubQueryPlan masterPlan = new SubQueryPlan(sp);

    /* Worker plan */
    GenericShuffleConsumer sc =
        new GenericShuffleConsumer(
            masterSource.getSchema(), id, new int[] {MyriaConstants.MASTER_ID});
    DbInsert insert = new DbInsert(sc, dest, true);
    Map<Integer, SubQueryPlan> workerPlans = Maps.newHashMap();
    for (int i : workers) {
      workerPlans.put(i, new SubQueryPlan(insert));
    }

    return new SubQuery(masterPlan, workerPlans);
  }

  /**
   * Construct a SubQuery that will fail on the master during initialization. Useful for testing failures.
   */
  public static final SubQuery failOnMasterInit() {
    /* Master plan */
    EOSSource src = new EOSSource();
    Operator fail = new InitFailureInjector(src);
    SinkRoot root = new SinkRoot(fail);

    Map<Integer, SubQueryPlan> workerPlans = Maps.newHashMap();

    return new SubQuery(new SubQueryPlan(root), workerPlans);
  }

  /**
   * Construct a SubQuery that will fail on one worker during initialization. Useful for testing failures.
   */
  public static final SubQuery failOnFirstWorkerInit(@Nonnull int[] workers) {
    Preconditions.checkElementIndex(1, workers.length);

    /* Master plan */
    SubQueryPlan masterPlan = new SubQueryPlan(new SinkRoot(new EOSSource()));

    /* Worker plans */
    Map<Integer, SubQueryPlan> workerPlans = Maps.newHashMap();
    /* First worker */
    workerPlans.put(
        workers[0], new SubQueryPlan(new SinkRoot(new InitFailureInjector(new EOSSource()))));
    return new SubQuery(masterPlan, workerPlans);
  }

  /**
   * Returns a {@link TupleBatchBuffer} containing the values 0 to {@code n-1}. The column is of type {@Link
   * Type#INT_TYPE} and the column name is {@code "val"}.
   *
   * @param n the number of values in the buffer.
   * @return a {@link TupleBatchBuffer} containing the values 0 to {@code n-1}
   */
  public static TupleBatchBuffer range(int n) {
    TupleBatchBuffer sourceBuffer = new TupleBatchBuffer(Schema.ofFields(Type.INT_TYPE, "val"));
    for (int i = 0; i < n; ++i) {
      sourceBuffer.putInt(0, i);
    }
    return sourceBuffer;
  }
}
