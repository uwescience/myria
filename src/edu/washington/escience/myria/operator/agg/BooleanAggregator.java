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
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator extends PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * Count, always of long type.
   */
  private long count;

  /**
   * Aggregate operations applicable for boolean columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   */
  public BooleanAggregator(final String aFieldName, final AggregationOp[] aggOps) {
    super(aFieldName, aggOps);
  }

  @Override
  public void add(final ReadableTable from, final int fromColumn) {
    Objects.requireNonNull(from, "from");
    add(from.asColumn(fromColumn));
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addBoolean(final boolean value) {
    if (needsCount) {
      count = LongMath.checkedAdd(count, 1);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) {
    Objects.requireNonNull(dest, "dest");
    int idx = destColumn;
    for (AggregationOp op : aggOps) {
      switch (op) {
        case COUNT:
          dest.putLong(idx, count);
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
  public void add(final ReadableTable table, final int column, final int row) {
    Objects.requireNonNull(table, "table");
    addBoolean(table.getBoolean(column, row));
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public void add(final ReadableColumn from) {
    Objects.requireNonNull(from, "from");
    if (needsCount) {
      count = LongMath.checkedAdd(count, from.size());
    }
  }

  @Override
  protected Set<AggregationOp> getAvailableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  protected Type getSumType() {
    throw new UnsupportedOperationException("SUM of Boolean values");
  }
}
