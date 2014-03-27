package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a DateTimeColumn.
 */
public final class DateTimeAggregator implements Aggregator<DateTime> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate operations. An binary-or of all the applicable aggregate operations, i.e. those in
   * {@link DateTimeAggregator#AVAILABLE_AGG}.
   * */
  private final int aggOps;

  /**
   * Count, always of long type.
   * */
  private long count;

  /**
   * min and max keeps the same data type as the aggregating column.
   * */
  private DateTime min, max;

  /**
   * Result schema. It's automatically generated according to the {@link DateTimeAggregator#aggOps}.
   * */
  private final Schema resultSchema;

  /**
   * Aggregate operations applicable for string columns.
   * */
  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_MAX | Aggregator.AGG_OP_MIN;

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public DateTimeAggregator(final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException(
          "Unsupported aggregation on string column. Only count, min and max are supported");
    }

    this.aggOps = aggOps;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types.add(Type.DATETIME_TYPE);
      names.add("min_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.DATETIME_TYPE);
      names.add("max_" + aFieldName);
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final DateTime value) {
    Objects.requireNonNull(value, "value");
    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, 1);
    }
    if (AggUtils.needsStats(aggOps)) {
      addDateTimeStats(value);
    }
  }

  @Override
  public void add(final ReadableColumn from) {
    final int numTuples = from.size();
    if (numTuples == 0) {
      return;
    }
    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, numTuples);
    }
    if (AggUtils.needsStats(aggOps)) {
      for (int row = 0; row < numTuples; ++row) {
        addDateTimeStats(from.getDateTime(row));
      }
    }
  }

  @Override
  public void add(final ReadableTable from, final int fromColumn) {
    add(from.asColumn(Objects.requireNonNull(fromColumn, "fromColumn")));
  }

  @Override
  public void add(final ReadableTable table, final int column, final int row) {
    add(Objects.requireNonNull(table, "table").getDateTime(column, row));
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   */
  private void addDateTimeStats(final DateTime value) {
    if (AggUtils.needsMin(aggOps)) {
      if ((min == null) || (min.compareTo(value) > 0)) {
        min = value;
      }
    }
    if (AggUtils.needsMax(aggOps)) {
      if ((max == null) || (max.compareTo(value) < 0)) {
        max = value;
      }
    }
  }

  @Override
  public void addObj(final Object value) {
    add((DateTime) value);
  }

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) {
    int idx = destColumn;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      dest.putLong(idx, count);
      idx++;
    }
    if ((aggOps & AGG_OP_MIN) != 0) {
      dest.putDateTime(idx, min);
      idx++;
    }
    if ((aggOps & AGG_OP_MAX) != 0) {
      dest.putDateTime(idx, max);
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public Type getType() {
    return Type.DATETIME_TYPE;
  }
}
