package edu.washington.escience.myriad.operator.agg;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.util.MathUtils;

/**
 * Knows how to compute some aggregates over a LongColumn.
 */
public final class LongAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;

  private long min, max, sum;
  private long count;
  private final Schema resultSchema;

  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG;

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  private LongAggregator(final int afield, final int aggOps, final Schema resultSchema) {
    this.resultSchema = resultSchema;
    this.afield = afield;
    this.aggOps = aggOps;
    sum = 0;
    count = 0;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
  }

  public LongAggregator(final int afield, final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on long column.");
    }
    this.afield = afield;
    this.aggOps = aggOps;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    sum = 0L;
    count = 0;
    int numAggOps = MathUtils.numBinaryOnesInInteger(aggOps);
    Type[] types = new Type[numAggOps];
    String[] names = new String[numAggOps];
    int idx = 0;
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types[idx] = Type.LONG_TYPE;
      names[idx] = "count(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types[idx] = Type.LONG_TYPE;
      names[idx] = "min(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types[idx] = Type.LONG_TYPE;
      names[idx] = "max(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_SUM) != 0) {
      types[idx] = Type.LONG_TYPE;
      names[idx] = "sum(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_AVG) != 0) {
      types[idx] = Type.DOUBLE_TYPE;
      names[idx] = "avg(" + aFieldName + ")";
      idx += 1;
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final TupleBatch tup) {

    int numTuples = tup.numTuples();
    if (numTuples > 0) {
      count += numTuples;
      for (int i = 0; i < numTuples; i++) {
        long x = tup.getLong(afield, i);
        sum += x;
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
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public LongAggregator freshCopyYourself() {
    return new LongAggregator(afield, aggOps, resultSchema);
  }
}
