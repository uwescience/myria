package edu.washington.escience.myria.operator.agg;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input this aggregator operates over. */
  private final int fromColumn;

  /**
   * Aggregate operations applicable for boolean columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param column the column being aggregated over.
   */
  public BooleanAggregator(final String aFieldName, final AggregationOp[] aggOps, final int column) {
    super(aFieldName, aggOps);
    fromColumn = column;
  }

  @Override
  public void add(final ReadableTable from, final Object state) {
    Objects.requireNonNull(from, "from");
    BooleanAggState b = (BooleanAggState) state;
    b.count += from.numTuples();
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added.
   * @param state the current state of the aggregate.
   */
  public void addBoolean(final boolean value, final Object state) {
    BooleanAggState b = (BooleanAggState) state;
    if (needsCount) {
      b.count = LongMath.checkedAdd(b.count, 1);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) {
    BooleanAggState b = (BooleanAggState) state;
    Objects.requireNonNull(dest, "dest");
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case COUNT:
          dest.putLong(idx, b.count);
          break;
        case AVG:
        case MAX:
        case MIN:
        case STDEV:
        case SUM:
          throw new UnsupportedOperationException("Aggregate " + op + " on type Boolean");
      }
      idx++;
    }
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  protected Type getSumType() {
    throw new UnsupportedOperationException("SUM of Boolean values");
  }

  @Override
  public void addRow(final ReadableTable from, final int row, final Object state)
      throws DbException {
    addBoolean(from.getBoolean(fromColumn, row), state);
  }

  @Override
  public Object getInitialState() {
    return new BooleanAggState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class BooleanAggState {
    /** The number of tuples seen so far. */
    private long count = 0;
  }
}
