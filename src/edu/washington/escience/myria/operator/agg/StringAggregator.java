package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;

/**
 * Knows how to compute some aggregate over a StringColumn.
 */
public final class StringAggregator implements Aggregator<String> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * aggregate field.
   * */
  private final int afield;

  /**
   * Aggregate operations. An binary-or of all the applicable aggregate operations, i.e. those in
   * {@link StringAggregator#AVAILABLE_AGG}.
   * */
  private final int aggOps;

  /**
   * Count, always of long type.
   * */
  private long count;

  /**
   * min and max keeps the same data type as the aggregating column.
   * */
  private String min, max;

  /**
   * avoid compute min if not required.
   * */
  private final boolean computeMin;
  /**
   * avoid compute max if not required.
   * */
  private final boolean computeMax;

  /**
   * Result schema. It's automatically generated according to the {@link StringAggregator#aggOps}.
   * */
  private final Schema resultSchema;

  /**
   * Aggregate operations applicable for string columns.
   * */
  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_MAX | Aggregator.AGG_OP_MIN;

  /**
   * This serves as the copy constructor.
   * 
   * @param afield the aggregate column.
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param resultSchema the result schema.
   * @param computeMin if min is required
   * @param computeMax if max is required
   * */
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

  /**
   * @param afield the aggregate column.
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
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
      names.add("count_" + aFieldName);
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      computeMin = true;
      types.add(Type.STRING_TYPE);
      names.add("min_" + aFieldName);
    } else {
      computeMin = false;
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types.add(Type.STRING_TYPE);
      names.add("max_" + aFieldName);
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
  public void add(final String value) {

    if (value != null) {
      count++;
      if (computeMin || computeMax) {
        final String r = value;
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

  @Override
  public void addObj(final Object value) {
    add((String) value);
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
