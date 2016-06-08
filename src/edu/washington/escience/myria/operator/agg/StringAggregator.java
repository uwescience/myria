package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a StringColumn.
 */
public final class StringAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for string columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG =
      ImmutableSet.of(AggregationOp.COUNT, AggregationOp.MAX, AggregationOp.MIN);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param column the column being aggregated over.
   */
  public StringAggregator(final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    StringAggState sstate = (StringAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }
    if (needsCount) {
      sstate.count = LongMath.checkedAdd(sstate.count, numTuples);
    }
    if (needsStats) {
      for (int i = 0; i < numTuples; ++i) {
        addStringStats(from.getString(fromColumn, i), sstate);
      }
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    Objects.requireNonNull(table, "table");
    StringAggState sstate = (StringAggState) state;
    if (needsCount) {
      sstate.count = LongMath.checkedAdd(sstate.count, 1);
    }
    if (needsStats) {
      addStringStats(table.getString(fromColumn, row), sstate);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addStringStats(final String value, final StringAggState state) {
    Objects.requireNonNull(value, "value");
    if (needsMin) {
      if ((state.min == null) || (state.min.compareTo(value) > 0)) {
        state.min = value;
      }
    }
    if (needsMax) {
      if (state.max == null || state.max.compareTo(value) < 0) {
        state.max = value;
      }
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) {
    StringAggState sstate = (StringAggState) state;
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case COUNT:
          dest.putLong(idx, sstate.count);
          break;
        case MAX:
          dest.putString(idx, sstate.max);
          break;
        case MIN:
          dest.putString(idx, sstate.min);
          break;
        case AVG:
        case STDEV:
        case SUM:
          throw new UnsupportedOperationException("Aggregate " + op + " on type String");
      }
    }
  }

  @Override
  protected Type getSumType() {
    throw new UnsupportedOperationException("SUM of String values");
  }

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public Object getInitialState() {
    return new StringAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class StringAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private String min = null;
    /** The maximum value in the aggregated column. */
    private String max = null;
  }
}
