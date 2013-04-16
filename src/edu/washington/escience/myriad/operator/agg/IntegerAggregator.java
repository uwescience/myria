package edu.washington.escience.myriad.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate column.
   * */
  private final int aColumn;
  /**
   * Aggregate operations. An binary-or of all the applicable aggregate operations, i.e. those in
   * {@link IntegerAggregator#AVAILABLE_AGG}.
   * */
  private final int aggOps;

  /**
   * min, max and sum, keeps the same data type as the aggregating column.
   * */
  private int min, max, sum;

  /** private temp variables for computing stdev. */
  private double sumSquared;

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
  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG | Aggregator.AGG_OP_STDEV;

  /**
   * This serves as the copy constructor.
   * 
   * @param afield the aggregate column.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param resultSchema the result schema.
   * */
  private IntegerAggregator(final int afield, final int aggOps, final Schema resultSchema) {
    this.resultSchema = resultSchema;
    aColumn = afield;
    this.aggOps = aggOps;
    sum = 0;
    count = 0;
    min = Integer.MAX_VALUE;
    max = Integer.MIN_VALUE;
    sumSquared = 0.0;
  }

  /**
   * @param afield the aggregate column.
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public IntegerAggregator(final int afield, final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on int column.");
    }
    aColumn = afield;
    this.aggOps = aggOps;
    min = Integer.MAX_VALUE;
    max = Integer.MIN_VALUE;
    sum = 0;
    count = 0;
    sumSquared = 0.0;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types.add(Type.INT_TYPE);
      names.add("min(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.INT_TYPE);
      names.add("max(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_SUM) != 0) {
      types.add(Type.INT_TYPE);
      names.add("sum(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_AVG) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("avg(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_STDEV) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("stdev(" + aFieldName + ")");
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public final void add(final TupleBatch tup) {

    final int numTuples = tup.numTuples();
    if (numTuples > 0) {
      count += numTuples;
      for (int i = 0; i < numTuples; i++) {
        final int x = tup.getInt(aColumn, i);
        sum += x;
        sumSquared += x * x;
        if (min > x) {
          min = x;
        }
        if (max < x) {
          max = x;
        }
      }
    }

  }

  @Override
  public final int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public final Aggregator freshCopyYourself() {
    return new IntegerAggregator(aColumn, aggOps, resultSchema);
  }

  @Override
  public final void getResult(final TupleBatchBuffer buffer, final int fromIndex) {
    int idx = fromIndex;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      buffer.put(idx, count);
      idx++;
    }
    if ((aggOps & AGG_OP_MIN) != 0) {
      buffer.put(idx, min);
      idx++;
    }
    if ((aggOps & AGG_OP_MAX) != 0) {
      buffer.put(idx, max);
      idx++;
    }
    if ((aggOps & AGG_OP_SUM) != 0) {
      buffer.put(idx, sum);
      idx++;
    }
    if ((aggOps & AGG_OP_AVG) != 0) {
      buffer.put(idx, sum * 1.0 / count);
      idx++;
    }
    if ((aggOps & AGG_OP_STDEV) != 0) {
      double stdev = Math.sqrt((sumSquared / count) - ((double) (sum) / count * sum / count));
      buffer.put(idx, stdev);
      idx++;
    }
  }

  @Override
  public final Schema getResultSchema() {
    return resultSchema;
  }
}
