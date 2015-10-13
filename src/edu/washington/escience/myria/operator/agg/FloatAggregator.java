package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a FloatColumn.
 */
public final class FloatAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for float columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT,
      AggregationOp.SUM, AggregationOp.MAX, AggregationOp.MIN, AggregationOp.AVG,
      AggregationOp.STDEV);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param column the column being aggregated over.
   */
  public FloatAggregator(final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    FloatAggState f = (FloatAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }

    if (needsCount) {
      f.count = LongMath.checkedAdd(f.count, numTuples);
    }

    if (!needsStats) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addFloatStats(from.getFloat(fromColumn, i), f);
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    Objects.requireNonNull(table, "table");
    FloatAggState f = (FloatAggState) state;
    if (needsCount) {
      f.count = LongMath.checkedAdd(f.count, 1);
    }
    if (needsStats) {
      addFloatStats(table.getFloat(fromColumn, row), f);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addFloatStats(final float value, final FloatAggState state) {
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
    FloatAggState f = (FloatAggState) state;
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case AVG:
          dest.putDouble(idx, f.sum * 1.0 / f.count);
          break;
        case COUNT:
          dest.putLong(idx, f.count);
          break;
        case MAX:
          dest.putFloat(idx, f.max);
          break;
        case MIN:
          dest.putFloat(idx, f.min);
          break;
        case STDEV:
          double first = f.sumSquared / f.count;
          double second = f.sum / f.count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putDouble(idx, f.sum);
          break;
      }
      idx++;
    }
  }

  @Override
  public Type getType() {
    return Type.FLOAT_TYPE;
  }

  @Override
  protected Type getSumType() {
    return Type.DOUBLE_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public Object getInitialState() {
    return new FloatAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class FloatAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private float min = Float.MAX_VALUE;
    /** The maximum value in the aggregated column. */
    private float max = Float.MIN_VALUE;
    /** The sum of values in the aggregated column. */
    private double sum = 0;
    /** private temp variables for computing stdev. */
    private double sumSquared = 0;
  }
}
