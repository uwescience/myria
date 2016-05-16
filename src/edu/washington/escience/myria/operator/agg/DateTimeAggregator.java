package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a DateTimeColumn.
 */
public final class DateTimeAggregator extends PrimitiveAggregator {

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
  public DateTimeAggregator(
      final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  /**
   * Add the specified value to this aggregator.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  public void addDateTime(final DateTime value, final Object state) {
    Objects.requireNonNull(value, "value");
    DateTimeAggState d = (DateTimeAggState) state;
    if (needsCount) {
      d.count = LongMath.checkedAdd(d.count, 1);
    }
    if (needsStats) {
      addDateTimeStats(value, d);
    }
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    DateTimeAggState d = (DateTimeAggState) state;
    final int numTuples = from.numTuples();
    if (numTuples == 0) {
      return;
    }
    if (needsCount) {
      d.count = LongMath.checkedAdd(d.count, numTuples);
    }
    if (needsStats) {
      for (int row = 0; row < numTuples; ++row) {
        addDateTimeStats(from.getDateTime(fromColumn, row), d);
      }
    }
  }

  @Override
  public void addRow(final ReadableTable table, final int row, final Object state) {
    addDateTime(Objects.requireNonNull(table, "table").getDateTime(fromColumn, row), state);
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   *
   * @param value the value to be added
   * @param state the state of the aggregate, which will be mutated.
   */
  private void addDateTimeStats(final DateTime value, final DateTimeAggState state) {
    Objects.requireNonNull(value, "value");
    if (needsMin) {
      if ((state.min == null) || (state.min.compareTo(value) > 0)) {
        state.min = value;
      }
    }
    if (needsMax) {
      if ((state.max == null) || (state.max.compareTo(value) < 0)) {
        state.max = value;
      }
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) {
    DateTimeAggState d = (DateTimeAggState) state;
    Objects.requireNonNull(dest, "dest");
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case COUNT:
          dest.putLong(idx, d.count);
          break;
        case MAX:
          dest.putDateTime(idx, d.max);
          break;
        case MIN:
          dest.putDateTime(idx, d.min);
          break;
        case AVG:
        case STDEV:
        case SUM:
          throw new UnsupportedOperationException("Aggregate " + op + " on type DateTime");
      }
      idx++;
    }
  }

  @Override
  protected Type getSumType() {
    throw new UnsupportedOperationException("SUM of DateTime values");
  }

  @Override
  public Type getType() {
    return Type.DATETIME_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public Object getInitialState() {
    return new DateTimeAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class DateTimeAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
    /** The minimum value in the aggregated column. */
    private DateTime min = null;
    /** The maximum value in the aggregated column. */
    private DateTime max = null;
  }
}
