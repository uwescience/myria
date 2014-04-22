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
 * Knows how to compute some aggregates over a FloatColumn.
 */
public final class FloatAggregator implements Aggregator<Float> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate operations. An binary-or of all the applicable aggregate operations, i.e. those in
   * {@link FloatAggregator#AVAILABLE_AGG}.
   * */
  private final int aggOps;

  /** The minimum value in the aggregated column. */
  private float min;
  /** The maximum value in the aggregated column. */
  private float max;
  /** The sum of values in the aggregated column. */
  private double sum;

  /** private temp variables for computing stdev. */
  private double sumSquared;

  /**
   * Count, always of long type.
   * */
  private long count;

  /**
   * Result schema. It's automatically generated according to the {@link FloatAggregator#aggOps}.
   * */
  private final Schema resultSchema;

  /**
   * Aggregate operations applicable for float columns.
   * */
  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG | Aggregator.AGG_OP_STDEV;

  /**
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public FloatAggregator(final String aFieldName, final int aggOps) {
    Objects.requireNonNull(aFieldName, "aFieldName");
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on float column.");
    }
    this.aggOps = aggOps;
    min = Float.MAX_VALUE;
    max = Float.MIN_VALUE;
    sum = 0.0;
    count = 0;
    sumSquared = 0.0;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types.add(Type.FLOAT_TYPE);
      names.add("min_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.FLOAT_TYPE);
      names.add("max_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_SUM) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("sum_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_AVG) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("avg_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_STDEV) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("stdev(" + aFieldName + ")");
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final Float value) {
    addFloat(Objects.requireNonNull(value, "value"));
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
      addFloatStats(from.getFloat(i));
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
    addFloat(table.getFloat(column, row));
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addFloat(final float value) {
    if (AggUtils.needsCount(aggOps)) {
      count = LongMath.checkedAdd(count, 1);
    }
    if (AggUtils.needsStats(aggOps)) {
      addFloatStats(value);
    }
  }

  /**
   * Helper function to add value to this aggregator. Note this does NOT update count.
   * 
   * @param value the value to be added
   */
  private void addFloatStats(final float value) {
    if (AggUtils.needsSum(aggOps)) {
      sum += value;
    }
    if (AggUtils.needsSumSq(aggOps)) {
      sumSquared += value * value;
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
      dest.putFloat(idx, min);
      idx++;
    }
    if ((aggOps & AGG_OP_MAX) != 0) {
      dest.putFloat(idx, max);
      idx++;
    }
    if ((aggOps & AGG_OP_SUM) != 0) {
      dest.putDouble(idx, sum);
      idx++;
    }
    if ((aggOps & AGG_OP_AVG) != 0) {
      dest.putDouble(idx, sum * 1.0 / count);
      idx++;
    }
    if ((aggOps & AGG_OP_STDEV) != 0) {
      double stdev = Math.sqrt((sumSquared / count) - ((sum) / count * sum / count));
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
    return Type.FLOAT_TYPE;
  }
}
