package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a DoubleColumn.
 */
public final class DoubleAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for double columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG =
      ImmutableSet.of(
          AggregationOp.COUNT,
          AggregationOp.SUM,
          AggregationOp.MAX,
          AggregationOp.MIN,
          AggregationOp.AVG,
          AggregationOp.STDEV);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param column the column being aggregated over.
   */
  public DoubleAggregator(final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    DoubleAggState d = (DoubleAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }
    if (needsCount) {
      d.count = LongMath.checkedAdd(d.count, numTuples);
    }
    if (!needsStats) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addDoubleStats(from.getDouble(fromColumn, i), d);
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    Objects.requireNonNull(table, "table");
    DoubleAggState d = (DoubleAggState) state;
    if (needsCount) {
      d.count = LongMath.checkedAdd(d.count, 1);
    }
    if (needsStats) {
      addDoubleStats(table.getDouble(fromColumn, row), d);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addDoubleStats(final double value, final DoubleAggState state) {
    if (needsSum) {
      state.sum += value;
    }
    if (needsSumSq) {
      state.sumSquared += value * value;
    }
    if (needsMin) {
      state.min = Math.min(state.min, value);
    }
    if (needsMax) {
      state.max = Math.max(state.max, value);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) {
    Objects.requireNonNull(dest, "dest");
    DoubleAggState d = (DoubleAggState) state;
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case AVG:
          dest.putDouble(idx, d.sum * 1.0 / d.count);
          break;
        case COUNT:
          dest.putLong(idx, d.count);
          break;
        case MAX:
          dest.putDouble(idx, d.max);
          break;
        case MIN:
          dest.putDouble(idx, d.min);
          break;
        case STDEV:
          double first = d.sumSquared / d.count;
          double second = d.sum / d.count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putDouble(idx, d.sum);
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

  @Override
  public Object getInitialState() {
    return new DoubleAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class DoubleAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private double min = Double.MAX_VALUE;
    /** The maximum value in the aggregated column. */
    private double max = Double.MIN_VALUE;
    /** The sum of values in the aggregated column. */
    private double sum = 0;
    /** private temp variables for computing stdev. */
    private double sumSquared = 0;
  }
}
