package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public final class IntegerAggregator implements PrimitiveAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate operations. An binary-or of all the applicable aggregate operations, i.e. those in
   * {@link IntegerAggregator#AVAILABLE_AGG}.
   * */
  private final int aggOps;

  /** The minimum value in the aggregated column. */
  private int min;
  /** The maximum value in the aggregated column. */
  private int max;
  /** The sum of values in the aggregated column. */
  private long sum;

  /** Private temp variables for computing stdev. */
  private long sumSquared;

  /**
   * Count, always of long type.
   * */
  private long count;

  /**
   * Result schema. It's automatically generated according to the {@link IntegerAggregator#aggOps}.
   * */
  private final Schema resultSchema;

  /**
   * Aggregate operations applicable for int columns.
   * */
  public static final int AVAILABLE_AGG = PrimitiveAggregator.AGG_OP_COUNT | PrimitiveAggregator.AGG_OP_SUM
      | PrimitiveAggregator.AGG_OP_MAX | PrimitiveAggregator.AGG_OP_MIN | PrimitiveAggregator.AGG_OP_AVG
      | PrimitiveAggregator.AGG_OP_STDEV;

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public IntegerAggregator(final String aFieldName, final int aggOps) {
    Objects.requireNonNull(aFieldName, "aFieldName");
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on int column.");
    }
    this.aggOps = aggOps;
    min = Integer.MAX_VALUE;
    max = Integer.MIN_VALUE;
    sum = 0;
    count = 0;
    sumSquared = 0L;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & PrimitiveAggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count_" + aFieldName);
    }
    if ((aggOps & PrimitiveAggregator.AGG_OP_MIN) != 0) {
      types.add(Type.INT_TYPE);
      names.add("min_" + aFieldName);
    }
    if ((aggOps & PrimitiveAggregator.AGG_OP_MAX) != 0) {
      types.add(Type.INT_TYPE);
      names.add("max_" + aFieldName);
    }
    if ((aggOps & PrimitiveAggregator.AGG_OP_SUM) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("sum_" + aFieldName);
    }
    if ((aggOps & PrimitiveAggregator.AGG_OP_AVG) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("avg_" + aFieldName);
    }
    if ((aggOps & PrimitiveAggregator.AGG_OP_STDEV) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("stdev_" + aFieldName);
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final ReadableColumn from) {
    Objects.requireNonNull(from, "from");
    final int numTuples = from.size();
    if (numTuples == 0) {
      return;
    }

    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, numTuples);
    }

    if (!AggUtils.needsStats(aggOps)) {
      return;
    }
    for (int i = 0; i < numTuples; i++) {
      addIntStats(from.getInt(i));
    }
  }

  @Override
  public void add(final ReadableTable from, final int fromColumn) {
    Objects.requireNonNull(from, "from");
    add(from.asColumn(fromColumn));
  }

  @Override
  public void add(final ReadableTable table, final int column, final int row) {
    Objects.requireNonNull(table, "table");
    addInt(table.getInt(column, row));
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addInt(final int value) {
    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, 1);
    }
    if (AggUtils.needsStats(aggOps)) {
      addIntStats(value);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   */
  private void addIntStats(final int value) {
    if (AggUtils.needsSum(aggOps)) {
      sum = LongMath.checkedAdd(sum, value);
    }
    if (AggUtils.needsSumSq(aggOps)) {
      // don't need to check value*value since value is an int
      sumSquared = LongMath.checkedAdd(sumSquared, ((long) value) * value);
    }
    if (AggUtils.needsMin(aggOps)) {
      min = Math.min(min, value);
    }
    if (AggUtils.needsMax(aggOps)) {
      max = Math.max(max, value);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) {
    Objects.requireNonNull(dest, "dest");
    int idx = destColumn;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      dest.putLong(idx, count);
      idx++;
    }
    if ((aggOps & AGG_OP_MIN) != 0) {
      dest.putInt(idx, min);
      idx++;
    }
    if ((aggOps & AGG_OP_MAX) != 0) {
      dest.putInt(idx, max);
      idx++;
    }
    if ((aggOps & AGG_OP_SUM) != 0) {
      dest.putLong(idx, sum);
      idx++;
    }
    if ((aggOps & AGG_OP_AVG) != 0) {
      dest.putDouble(idx, ((double) sum) / count);
      idx++;
    }
    if ((aggOps & AGG_OP_STDEV) != 0) {
      double first = ((double) sumSquared) / count;
      double second = ((double) sum) / count;
      double stdev = Math.sqrt(first - second * second);
      dest.putDouble(idx, stdev);
      idx++;
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
  }
}
