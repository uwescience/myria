package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a LongColumn.
 */
public final class LongAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for long columns.
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
  public LongAggregator(final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    LongAggState lstate = (LongAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }
    if (needsCount) {
      lstate.count = LongMath.checkedAdd(lstate.count, numTuples);
    }

    if (!needsStats) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addLongStats(from.getLong(fromColumn, i), lstate);
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    Objects.requireNonNull(table, "table");
    LongAggState lstate = (LongAggState) state;
    if (needsCount) {
      lstate.count = LongMath.checkedAdd(lstate.count, 1);
    }
    if (needsStats) {
      addLongStats(table.getLong(fromColumn, row), lstate);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addLongStats(final long value, final LongAggState state) {
    if (needsSum) {
      state.sum = LongMath.checkedAdd(state.sum, value);
    }
    if (needsSumSq) {
      state.sumSquared =
          LongMath.checkedAdd(state.sumSquared, LongMath.checkedMultiply(value, value));
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

    LongAggState lstate = (LongAggState) state;
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case AVG:
          dest.putDouble(idx, lstate.sum * 1.0 / lstate.count);
          break;
        case COUNT:
          dest.putLong(idx, lstate.count);
          break;
        case MAX:
          dest.putLong(idx, lstate.max);
          break;
        case MIN:
          dest.putLong(idx, lstate.min);
          break;
        case STDEV:
          double first = ((double) lstate.sumSquared) / lstate.count;
          double second = ((double) lstate.sum) / lstate.count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putLong(idx, lstate.sum);
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

  @Override
  public Object getInitialState() {
    return new LongAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class LongAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private long min = Long.MAX_VALUE;
    /** The maximum value in the aggregated column. */
    private long max = Long.MIN_VALUE;
    /** The sum of values in the aggregated column. */
    private long sum = 0;
    /** private temp variables for computing stdev. */
    private long sumSquared = 0;
  }
}
