package edu.washington.escience.myriad.operator.agg;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class DoubleAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;

  private double min, max, sum;
  private int count;

  private final Schema resultSchema;

  public static int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG;

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  private DoubleAggregator(int afield, int aggOps, Schema resultSchema) {
    this.resultSchema = resultSchema;
    this.afield = afield;
    this.aggOps = aggOps;
    count = 0;
    max = Double.MIN_VALUE;
    min = Double.MAX_VALUE;
    sum = 0;
  }

  public DoubleAggregator(int afield, String aFieldName, int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on double column.");
    }

    this.afield = afield;
    this.aggOps = aggOps;
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
    sum = 0.0;
    count = 0;
    int numAggOps = ParallelUtility.numBinaryOnesInInteger(aggOps);
    Type[] types = new Type[numAggOps];
    String[] names = new String[numAggOps];
    int idx = 0;
    if ((aggOps & Aggregator.AGG_OP_COUNT) != 0) {
      types[idx] = Type.INT_TYPE;
      names[idx] = "count(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      types[idx] = Type.INT_TYPE;
      names[idx] = "min(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types[idx] = Type.INT_TYPE;
      names[idx] = "max(" + aFieldName + ")";
      idx += 1;
    }
    if ((aggOps & Aggregator.AGG_OP_SUM) != 0) {
      types[idx] = Type.INT_TYPE;
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

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   * 
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  @Override
  public void add(_TupleBatch tup) {

    count += tup.numOutputTuples();
    DoubleColumn rawData = (DoubleColumn) tup.outputRawData().get(afield);
    int numTuples = rawData.size();
    for (int i = 0; i < numTuples; i++) {
      double x = rawData.getDouble(i);
      sum += x;
      if (Double.compare(x, min) < 0) {
        min = x;
      }
      if (Double.compare(x, max) > 0) {
        max = x;
      }
    }

  }

  /**
   * Output count, sum, avg, min, max
   * 
   * */
  @Override
  public void getResult(TupleBatchBuffer buffer, final int fromIndex) {
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
  public DoubleAggregator freshCopyYourself() {
    return new DoubleAggregator(afield, aggOps, resultSchema);
  }
}
