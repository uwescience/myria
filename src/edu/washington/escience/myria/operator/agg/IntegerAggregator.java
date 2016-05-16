package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public final class IntegerAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for int columns.
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
  public IntegerAggregator(
      final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    IntAggState istate = (IntAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }

    if (needsCount) {
      istate.count = LongMath.checkedAdd(istate.count, numTuples);
    }

    if (!needsStats) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addIntStats(from.getInt(fromColumn, i), istate);
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    Objects.requireNonNull(table, "table");
    IntAggState istate = (IntAggState) state;
    if (needsCount) {
      istate.count = LongMath.checkedAdd(istate.count, 1);
    }
    if (needsStats) {
      addIntStats(table.getInt(fromColumn, row), istate);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addIntStats(final int value, final IntAggState state) {
    if (needsSum) {
      state.sum = LongMath.checkedAdd(state.sum, value);
    }
    if (needsSumSq) {
      // don't need to check value*value since value is an int
      state.sumSquared = LongMath.checkedAdd(state.sumSquared, ((long) value) * value);
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
    IntAggState istate = (IntAggState) state;
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case AVG:
          dest.putDouble(idx, istate.sum * 1.0 / istate.count);
          break;
        case COUNT:
          dest.putLong(idx, istate.count);
          break;
        case MAX:
          dest.putInt(idx, istate.max);
          break;
        case MIN:
          dest.putInt(idx, istate.min);
          break;
        case STDEV:
          double first = ((double) istate.sumSquared) / istate.count;
          double second = ((double) istate.sum) / istate.count;
          double stdev = Math.sqrt(first - second * second);
          dest.putDouble(idx, stdev);
          break;
        case SUM:
          dest.putLong(idx, istate.sum);
          break;
      }
      idx++;
    }
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
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
    return new IntAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class IntAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private int min = Integer.MAX_VALUE;
    /** The maximum value in the aggregated column. */
    private int max = Integer.MIN_VALUE;
    /** The sum of values in the aggregated column. */
    private long sum = 0;
    /** private temp variables for computing stdev. */
    private long sumSquared = 0;
  }
}
