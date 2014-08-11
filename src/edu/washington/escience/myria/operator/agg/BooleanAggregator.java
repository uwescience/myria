package edu.washington.escience.myria.operator.agg;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator implements PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Count, always of long type.
   */
  private long count;
  /**
   * Result schema. It's automatically generated according to the {@link BooleanAggregator#aggOps}.
   */
  private final Schema resultSchema;
  /**
   * Aggregate operations. A set of all valid aggregation operations in {@link PrimitiveAggregator}.
   * 
   * Note that we use a {@link LinkedHashSet} to ensure that the iteration order is consistent!
   */
  private final LinkedHashSet<AggregationOp> aggOps;

  /**
   * Aggregate operations applicable for boolean columns.
   */
  public static final Set<AggregationOp> AVAILABLE_AGG = ImmutableSet.of(AggregationOp.COUNT);

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   */
  public BooleanAggregator(final String aFieldName, final AggregationOp[] aggOps) {
    Objects.requireNonNull(aFieldName, "aFieldName");
    if (aggOps.length == 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    this.aggOps = new LinkedHashSet<>(Arrays.asList(aggOps));
    if (!AVAILABLE_AGG.containsAll(this.aggOps)) {
      throw new IllegalArgumentException("Unsupported aggregation(s) on boolean column: "
          + Sets.difference(this.aggOps, AVAILABLE_AGG));
    }

    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    for (AggregationOp op : this.aggOps) {
      switch (op) {
        case COUNT:
          types.add(Type.LONG_TYPE);
          names.add("count_" + aFieldName);
          break;
        case AVG:
        case MAX:
        case MIN:
        case STDEV:
        case SUM:
          throw new UnsupportedOperationException("Aggregate " + op + " on type Boolean");
      }
    }
    resultSchema = new Schema(types.build(), names.build());
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
    if (AggUtils.needsCount(aggOps)) {
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
  public Schema getResultSchema() {
    return resultSchema;
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
    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, from.size());
    }
  }
}
