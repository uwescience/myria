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
 * Knows how to compute some aggregates over a DoubleColumn.
 */
public final class DoubleAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The minimum value in the aggregated column. */
  private double min;
  /** The maximum value in the aggregated column. */
  private double max;
  /** The sum of values in the aggregated column. */
  private double sum;

  /** private temp variables for computing stdev. */
  private double sumSquared;

  /**
   * Count, always of long type.
   */
  private long count;

  /**
   * Aggregate operations applicable for double columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT, AggregationOp.SUM,
      AggregationOp.MAX, AggregationOp.MIN, AggregationOp.AVG, AggregationOp.STDEV);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   */
  public DoubleAggregator(final String aFieldName, final AggregationOp[] aggOps) {
    super(aFieldName, aggOps);

    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
    sum = 0.0;
    count = 0;
    sumSquared = 0.0;
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
      addDoubleStats(from.getDouble(i));
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
    addDouble(table.getDouble(column, row));
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addDouble(final double value) {
    if (needsCount) {
      count = LongMath.checkedAdd(count, 1);
    }
    if (needsStats) {
      addDoubleStats(value);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   */
  private void addDoubleStats(final double value) {
    if (needsSum) {
      sum += value;
    }
    if (needsSumSq) {
      sumSquared += value * value;
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
          dest.putDouble(idx, max);
          break;
        case MIN:
          dest.putDouble(idx, min);
          break;
        case STDEV:
          double first = sumSquared / count;
          double second = sum / count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putDouble(idx, sum);
          break;
      }
      idx++;
    }
  }

  @Override
  public Type getType() {
    return Type.DOUBLE_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  protected Type getSumType() {
    return Type.DOUBLE_TYPE;
  }
}
