package edu.washington.escience.myriad.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregates over a FloatColumn.
 */
public final class FloatAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;

  private float min, max, sum;
  private long count;
  private final Schema resultSchema;

  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG;

  private FloatAggregator(final int afield, final int aggOps, final Schema resultSchema) {
    this.resultSchema = resultSchema;
    this.afield = afield;
    this.aggOps = aggOps;
    sum = 0;
    count = 0;
    min = Float.MAX_VALUE;
    max = Float.MIN_VALUE;
  }

  public FloatAggregator(final int afield, final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on float column.");
    }
    this.afield = afield;
    this.aggOps = aggOps;
    min = Float.MAX_VALUE;
    max = Float.MIN_VALUE;
    sum = 0.0f;
    count = 0;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types.add(Type.FLOAT_TYPE);
      names.add("min(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.FLOAT_TYPE);
      names.add("max(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_SUM) != 0) {
      types.add(Type.FLOAT_TYPE);
      names.add("sum(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_AVG) != 0) {
      types.add(Type.DOUBLE_TYPE);
      names.add("avg(" + aFieldName + ")");
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final TupleBatch tup) {

    final int numTuples = tup.numTuples();
    if (numTuples > 0) {
      count += numTuples;
      for (int i = 0; i < numTuples; i++) {
        final float x = tup.getFloat(afield, i);
        sum += x;
        if (Float.compare(x, min) < 0) {
          min = x;
        }
        if (Float.compare(x, max) > 0) {
          max = x;
        }
      }
    }
  }

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public FloatAggregator freshCopyYourself() {
    return new FloatAggregator(afield, aggOps, resultSchema);
  }

  @Override
  public void getResult(final TupleBatchBuffer buffer, final int fromIndex) {
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
    buffer.put(0, count);
    buffer.put(1, sum);
    buffer.put(2, sum / count);
    buffer.put(3, min);
    buffer.put(4, min);
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }
}
