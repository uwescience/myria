package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a LongColumn.
 */
public final class LongAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The minimum value in the aggregated column. */
  private long min;
  /** The maximum value in the aggregated column. */
  private long max;
  /** The sum of values in the aggregated column. */
  private long sum;

  /** Private temp variables for computing stdev. */
  private long sumSquared;

  /**
   * Count, always of long type.
   */
  private long count;

  /**
   * Aggregate operations applicable for long columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT, AggregationOp.SUM,
      AggregationOp.MAX, AggregationOp.MIN, AggregationOp.AVG, AggregationOp.STDEV);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   */
  public LongAggregator(final String aFieldName, final AggregationOp[] aggOps) {
    super(aFieldName, aggOps);

    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    sum = 0L;
    count = 0;
    sumSquared = 0;
  }

  @Override
  public void add(final ReadableColumn from) {
    Objects.requireNonNull(from, "from");
    final int numTuples = from.size();
    if (numTuples == 0) {
      return;
    }
    if (needsCount) {
      count = LongMath.checkedAdd(count, numTuples);
    }

    if (!needsStats) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addLongStats(from.getLong(i));
    }
  }

  @Override
  public void add(final ReadableTable from, final int fromColumn) {
    Objects.requireNonNull(from, "from");
    add(from.asColumn(fromColumn));
  }

  @Override
  public void add(final ReadableTable table, final int column, final int row) {
    Objects.requireNonNull(table, "table");
    addLong(table.getLong(column, row));
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addLong(final long value) {
    if (needsCount) {
      count = LongMath.checkedAdd(count, 1);
    }
    if (needsStats) {
      addLongStats(value);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   */
  private void addLongStats(final long value) {
    if (needsSum) {
      sum = LongMath.checkedAdd(sum, value);
    }
    if (needsSumSq) {
      sumSquared = LongMath.checkedAdd(sumSquared, LongMath.checkedMultiply(value, value));
    }
    if (needsMin) {
      min = Math.min(min, value);
    }
    if (needsMax) {
      max = Math.max(max, value);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) {
    Objects.requireNonNull(dest, "dest");
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case AVG:
          dest.putDouble(idx, sum * 1.0 / count);
          break;
        case COUNT:
          dest.putLong(idx, count);
          break;
        case MAX:
          dest.putLong(idx, max);
          break;
        case MIN:
          dest.putLong(idx, min);
          break;
        case STDEV:
          double first = ((double) sumSquared) / count;
          double second = ((double) sum) / count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putLong(idx, sum);
          break;
      }
      idx++;
    }
  }

  @Override
  public Type getType() {
    return Type.LONG_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  protected Type getSumType() {
    return Type.LONG_TYPE;
  }
}
