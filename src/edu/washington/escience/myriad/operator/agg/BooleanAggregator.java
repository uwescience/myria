package edu.washington.escience.myriad.operator.agg;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator implements Aggregator {

  /**
   * java Serialization id.
   * */
  private static final long serialVersionUID = 1L;

  private int count;
  private final Schema resultSchema;
  private final int aggOps;

  public static int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT;

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  private BooleanAggregator(int aggOps, Schema resultSchema) {
    this.resultSchema = resultSchema;
    this.aggOps = aggOps;
    count = 0;
  }

  public BooleanAggregator(int afield, String aFieldName, int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on boolean column. Only count is supported");
    }

    this.aggOps = aggOps;

    int numAggOps = ParallelUtility.numBinaryOnesInInteger(aggOps);
    Type[] types = new Type[numAggOps];
    String[] names = new String[numAggOps];
    int idx = 0;
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types[idx] = Type.INT_TYPE;
      names[idx] = "count(" + aFieldName + ")";
      idx += 1;
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(_TupleBatch tup) {
    count += tup.numOutputTuples();
  }

  @Override
  public void getResult(TupleBatchBuffer outputBuffer, final int fromIndex) {
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

  @Override
  public BooleanAggregator freshCopyYourself() {
    return new BooleanAggregator(aggOps, resultSchema);
  }
}
