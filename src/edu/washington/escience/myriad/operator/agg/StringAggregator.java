package edu.washington.escience.myriad.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregate over a StringColumn.
 */
public final class StringAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;
  private int count;
  private String min;
  private final boolean computeMin;
  private String max;
  private final boolean computeMax;
  private final Schema resultSchema;

  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_MAX | Aggregator.AGG_OP_MIN;

  private StringAggregator(final int afield, final int aggOps, final boolean computeMin, final boolean computeMax,
      final Schema resultSchema) {
    this.afield = afield;
    this.aggOps = aggOps;
    this.computeMax = computeMax;
    this.computeMin = computeMin;
    this.resultSchema = resultSchema;
    min = null;
    max = null;
    count = 0;
  }

  public StringAggregator(final int afield, final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException(
          "Unsupported aggregation on string column. Only count, min and max are supported");
    }

    this.afield = afield;
    this.aggOps = aggOps;
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count(" + aFieldName + ")");
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      computeMin = true;
      types.add(Type.STRING_TYPE);
      names.add("min(" + aFieldName + ")");
    } else {
      computeMin = false;
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.STRING_TYPE);
      names.add("max(" + aFieldName + ")");
      computeMax = true;
    } else {
      computeMax = false;
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(final TupleBatch tup) {

    final int numTuples = tup.numTuples();
    if (numTuples > 0) {
      count += numTuples;
      if (computeMin || computeMax) {
        for (int i = 0; i < numTuples; i++) {
          final String r = tup.getString(afield, i);
          if (computeMin) {
            if (min == null) {
              min = r;
            } else if (r.compareTo(min) < 0) {
              min = r;
            }
          }
          if (computeMax) {
            if (max == null) {
              max = r;
            } else if (r.compareTo(max) > 0) {
              max = r;
            }
          }
        }
      }
    }

  }

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public StringAggregator freshCopyYourself() {
    return new StringAggregator(afield, aggOps, computeMin, computeMax, resultSchema);
  }

  @Override
  public void getResult(final TupleBatchBuffer buffer, final int fromIndex) {
    int idx = fromIndex;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      buffer.put(idx, count);
      idx++;
    }
    if (computeMin) {
      buffer.put(idx, min);
      idx++;
    }
    if (computeMax) {
      buffer.put(idx, max);
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }
}
