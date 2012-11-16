package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.agg.Aggregate;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myriad.systemtest.SystemTestBase;
import edu.washington.escience.myriad.table._TupleBatch;

public class AggregateTest {

  public TupleBatchBuffer generateRandomTuples(int numTuples) {
    String[] names = SystemTestBase.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    long[] ids = SystemTestBase.randomLong(1000, 1005, names.length);

    final Schema schema = new Schema(new Type[] { Type.LONG_TYPE, Type.STRING_TYPE }, new String[] { "id", "name" });

    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testNoGroupCount() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Aggregate agg = new Aggregate(new TupleSource(testBase), new int[] { 1 }, new int[] { Aggregator.AGG_OP_COUNT });
    agg.open();
    _TupleBatch tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(numTuples, ((Integer) tb.getObject(0, 0)).intValue());
    }
  }

  public static Object min(TupleBatchBuffer tbb, int column) {
    List<TupleBatch> tbs = tbb.getAll();
    Comparable min = null;
    min = (Comparable) tbb.getAll().get(0).getColumn(column).get(0);
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      Column c = tb.outputRawData().get(column);
      for (int i = 0; i < numTuples; i++) {
        Comparable current = (Comparable) c.get(i);
        if (min.compareTo(current) > 0) {
          min = current;
        }
      }
    }
    return min;
  }

  public static Object max(TupleBatchBuffer tbb, int column) {
    List<TupleBatch> tbs = tbb.getAll();
    Comparable max = null;
    max = (Comparable) tbb.getAll().get(0).getColumn(column).get(0);
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      Column c = tb.outputRawData().get(column);
      for (int i = 0; i < numTuples; i++) {
        Comparable current = (Comparable) c.get(i);
        if (max.compareTo(current) < 0) {
          max = current;
        }
      }
    }
    return max;
  }

  public static long sumLong(TupleBatchBuffer tbb, int column) {
    List<TupleBatch> tbs = tbb.getAll();
    long sum = 0;
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      Column c = tb.outputRawData().get(column);
      for (int i = 0; i < numTuples; i++) {
        Long current = (Long) c.get(i);
        sum += current;
      }
    }
    return sum;
  }

  @Test
  public void testNoGroupMin() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Long minID = (Long) min(testBase, 0);
    String minName = (String) min(testBase, 1);

    Aggregate agg = new Aggregate(new TupleSource(testBase), new int[] { 0 }, new int[] { Aggregator.AGG_OP_MIN });
    agg.open();
    _TupleBatch tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(minID, tb.outputRawData().get(0).get(0));
    }
    agg.close();

    agg = new Aggregate(new TupleSource(testBase), new int[] { 1 }, new int[] { Aggregator.AGG_OP_MIN });
    agg.open();
    tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(minName, tb.outputRawData().get(0).get(0));
    }
  }

  @Test
  public void testNoGroupMax() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Long maxID = (Long) max(testBase, 0);
    String maxName = (String) max(testBase, 1);
    Aggregate agg = new Aggregate(new TupleSource(testBase), new int[] { 0 }, new int[] { Aggregator.AGG_OP_MAX });
    agg.open();
    _TupleBatch tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(maxID, tb.outputRawData().get(0).get(0));
    }
    agg.close();

    agg = new Aggregate(new TupleSource(testBase), new int[] { 1 }, new int[] { Aggregator.AGG_OP_MAX });
    agg.open();
    tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(maxName, tb.outputRawData().get(0).get(0));
    }
  }

  @Test
  public void testNoGroupSum() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Long sumID = sumLong(testBase, 0);
    Aggregate agg = new Aggregate(new TupleSource(testBase), new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM });
    agg.open();
    _TupleBatch tb = null;
    while ((tb = agg.next()) != null) {
      assertEquals(sumID, tb.outputRawData().get(0).get(0));
    }
    agg.close();
  }

  @Test
  public void testNoGroupAvg() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Long sumID = sumLong(testBase, 0);
    Aggregate agg = new Aggregate(new TupleSource(testBase), new int[] { 0 }, new int[] { Aggregator.AGG_OP_AVG });
    agg.open();
    _TupleBatch tb = null;
    while ((tb = agg.next()) != null) {
      assertTrue(Double.compare(sumID * 1.0 / numTuples, (Double) tb.outputRawData().get(0).get(0)) == 0);
    }
    agg.close();
  }

  public static HashMap<SystemTestBase.Tuple, Integer> groupBySumLongColumn(TupleBatchBuffer source, int groupByColumn,
      int aggColumn) {
    List<TupleBatch> tbs = source.getAll();
    HashMap<Object, Long> sum = new HashMap<Object, Long>();
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      List<Column> rawData = tb.outputRawData();
      for (int i = 0; i < numTuples; i++) {
        Object groupByValue = rawData.get(groupByColumn).get(i);
        Long aggValue = (Long) rawData.get(aggColumn).get(i);
        Long currentSum = sum.get(groupByValue);
        if (currentSum == null) {
          currentSum = 0l;
        }
        sum.put(groupByValue, currentSum + aggValue);
      }
    }
    HashMap<SystemTestBase.Tuple, Integer> result = new HashMap<SystemTestBase.Tuple, Integer>();

    for (Map.Entry<Object, Long> e : sum.entrySet()) {
      Object gValue = e.getKey();
      Long sumV = e.getValue();
      SystemTestBase.Tuple t = new SystemTestBase.Tuple(2);
      t.set(0, (Comparable) gValue);
      t.set(1, sumV);
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<SystemTestBase.Tuple, Integer> groupByCount(TupleBatchBuffer source, int groupByColumn) {
    List<TupleBatch> tbs = source.getAll();
    HashMap<Object, Integer> count = new HashMap<Object, Integer>();
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      List<Column> rawData = tb.outputRawData();
      for (int i = 0; i < numTuples; i++) {
        Object groupByValue = rawData.get(groupByColumn).get(i);
        Integer currentCount = count.get(groupByValue);
        if (currentCount == null) {
          currentCount = 0;
        }
        count.put(groupByValue, currentCount++);
      }
    }
    HashMap<SystemTestBase.Tuple, Integer> result = new HashMap<SystemTestBase.Tuple, Integer>();

    for (Map.Entry<Object, Integer> e : count.entrySet()) {
      Object gValue = e.getKey();
      Integer countV = e.getValue();
      SystemTestBase.Tuple t = new SystemTestBase.Tuple(2);
      t.set(0, (Comparable) gValue);
      t.set(1, countV);
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<SystemTestBase.Tuple, Integer> groupByAvgLongColumn(TupleBatchBuffer source, int groupByColumn,
      int aggColumn) {
    List<TupleBatch> tbs = source.getAll();
    HashMap<Object, Long> sum = new HashMap<Object, Long>();
    HashMap<Object, Integer> count = new HashMap<Object, Integer>();
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      List<Column> rawData = tb.outputRawData();
      for (int i = 0; i < numTuples; i++) {
        Object groupByValue = rawData.get(groupByColumn).get(i);
        Long aggValue = (Long) rawData.get(aggColumn).get(i);
        Long currentSum = sum.get(groupByValue);
        if (currentSum == null) {
          currentSum = 0l;
          count.put(groupByValue, 1);
        } else {
          count.put(groupByValue, count.get(groupByValue) + 1);
        }
        sum.put(groupByValue, currentSum + aggValue);
      }
    }
    HashMap<SystemTestBase.Tuple, Integer> result = new HashMap<SystemTestBase.Tuple, Integer>();

    for (Map.Entry<Object, Long> e : sum.entrySet()) {
      Object gValue = e.getKey();
      Long sumV = e.getValue();
      SystemTestBase.Tuple t = new SystemTestBase.Tuple(2);
      t.set(0, (Comparable) gValue);
      t.set(1, sumV * 1.0 / count.get(gValue));
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<SystemTestBase.Tuple, Integer> groupByMin(TupleBatchBuffer source, int groupByColumn,
      int aggColumn) {
    List<TupleBatch> tbs = source.getAll();
    HashMap<Object, Comparable> min = new HashMap<Object, Comparable>();
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      List<Column> rawData = tb.outputRawData();
      for (int i = 0; i < numTuples; i++) {
        Object groupByValue = rawData.get(groupByColumn).get(i);
        Comparable aggValue = (Comparable) rawData.get(aggColumn).get(i);
        Object currentMin = min.get(groupByValue);
        if (currentMin == null) {
          min.put(groupByValue, aggValue);
        } else if (aggValue.compareTo(currentMin) < 0) {
          min.put(groupByValue, aggValue);
        }
      }
    }
    HashMap<SystemTestBase.Tuple, Integer> result = new HashMap<SystemTestBase.Tuple, Integer>();

    for (Map.Entry<Object, Comparable> e : min.entrySet()) {
      Object gValue = e.getKey();
      Comparable minV = e.getValue();
      SystemTestBase.Tuple t = new SystemTestBase.Tuple(2);
      t.set(0, (Comparable) gValue);
      t.set(1, minV);
      result.put(t, 1);
    }
    return result;
  }

  public static HashMap<SystemTestBase.Tuple, Integer> groupByMax(TupleBatchBuffer source, int groupByColumn,
      int aggColumn) {
    List<TupleBatch> tbs = source.getAll();
    HashMap<Object, Comparable> max = new HashMap<Object, Comparable>();
    for (_TupleBatch tb : tbs) {
      int numTuples = tb.numOutputTuples();
      List<Column> rawData = tb.outputRawData();
      for (int i = 0; i < numTuples; i++) {
        Object groupByValue = rawData.get(groupByColumn).get(i);
        Comparable aggValue = (Comparable) rawData.get(aggColumn).get(i);
        Comparable currentMax = max.get(groupByValue);
        if (currentMax == null) {
          max.put(groupByValue, aggValue);
        } else if (aggValue.compareTo(currentMax) > 0) {
          max.put(groupByValue, aggValue);
        }
      }
    }
    HashMap<SystemTestBase.Tuple, Integer> result = new HashMap<SystemTestBase.Tuple, Integer>();

    for (Map.Entry<Object, Comparable> e : max.entrySet()) {
      Object gValue = e.getKey();
      Comparable maxV = e.getValue();
      SystemTestBase.Tuple t = new SystemTestBase.Tuple(2);
      t.set(0, (Comparable) gValue);
      t.set(1, maxV);
      result.put(t, 1);
    }
    return result;
  }

  @Test
  public void testSingleGroupAvg() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    Long sumID = sumLong(testBase, 0);
    // group by name, aggregate on id
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_AVG });
    agg.open();
    _TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupByAvgLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupSum() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_SUM });
    agg.open();
    _TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupBySumLongColumn(testBase, 1, 0), actualResult);
  }

  @Test
  public void testSingleGroupMin() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MIN });
    agg.open();
    _TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupByMin(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MIN });
    agg.open();
    tb = null;
    result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupByMin(testBase, 0, 1), actualResult);
  }

  @Test
  public void testSingleGroupMax() throws DbException {
    int randInt = new Random().nextInt();
    if (randInt < 0) {
      randInt = -randInt;
    }
    int numTuples = randInt % 200000;

    TupleBatchBuffer testBase = generateRandomTuples(numTuples);
    // group by name, aggregate on id
    SingleGroupByAggregate agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 0 }, 1, new int[] { Aggregator.AGG_OP_MAX });
    agg.open();
    _TupleBatch tb = null;
    TupleBatchBuffer result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    HashMap<SystemTestBase.Tuple, Integer> actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupByMax(testBase, 1, 0), actualResult);

    agg =
        new SingleGroupByAggregate(new TupleSource(testBase), new int[] { 1 }, 0, new int[] { Aggregator.AGG_OP_MAX });
    agg.open();
    tb = null;
    result = new TupleBatchBuffer(agg.getSchema());
    while ((tb = agg.next()) != null) {
      result.putAll(tb);
    }
    agg.close();
    actualResult = SystemTestBase.tupleBatchToTupleBag(result);
    SystemTestBase.assertTupleBagEqual(groupByMax(testBase, 0, 1), actualResult);
  }
}
