package edu.washington.escience.myriad.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Count, always of long type.
   * */
  private long count;
  /**
   * Result schema. It's automatically generated according to the {@link BooleanAggregator#aggOps}.
   * */
  private final Schema resultSchema;
  /**
   * Aggregate operations. An binary-or of all the operations in {@link Aggregator}.
   * */
  private final int aggOps;

  /**
   * Aggregate operations applicable for boolean columns.
   * */
  public static final int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT;

  /**
   * This serves as the copy constructor.
   * 
   * @param aggOps the aggregate operation to simultaneously compute.
   * @param resultSchema the result schema.
   * */
  private BooleanAggregator(final int aggOps, final Schema resultSchema) {
    this.resultSchema = resultSchema;
    this.aggOps = aggOps;
    count = 0;
  }

  /**
   * @param afield only count is supported on boolean columns, so afield is actually useless.
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public BooleanAggregator(final int afield, final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on boolean column. Only count is supported");
    }

    this.aggOps = aggOps;

    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types.add(Type.LONG_TYPE);
      names.add("count_" + aFieldName);
    }
    resultSchema = new Schema(types.build(), names.build());
  }

  @Override
  public void add(final TupleBatch tup) {
    count += tup.numTuples();
  }

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public BooleanAggregator freshCopyYourself() {
    return new BooleanAggregator(aggOps, resultSchema);
  }

  @Override
  public void getResult(final TupleBatchBuffer outputBuffer, final int fromIndex) {
    int idx = fromIndex;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      outputBuffer.put(idx, count);
      idx++;
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }
}
