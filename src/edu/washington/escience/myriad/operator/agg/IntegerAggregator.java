package edu.washington.escience.myriad.operator.agg;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;

  private int min, max, sum, count;

  private final Schema resultSchema;

  public static int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_MAX
      | Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_AVG;

  public IntegerAggregator(int afield, String aFieldName, int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException("Unsupported aggregation on int column.");
    }
    this.afield = afield;
    this.aggOps = aggOps;
    min = Integer.MAX_VALUE;
    max = Integer.MIN_VALUE;
    sum = 0;
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

  @Override
  public int available() {
    return AVAILABLE_AGG;
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   * 
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  @Override
  public void add(_TupleBatch tup) {

    count += tup.numOutputTuples();
    IntColumn rawData = (IntColumn) tup.outputRawData().get(afield);
    int numTuples = rawData.size();
    for (int i = 0; i < numTuples; i++) {
      int x = rawData.getInt(i);
      sum += x;
      if (min > x) {
        min = x;
      }
      if (max < x) {
        max = x;
      }
    }

  }

  /**
   * Output count, sum, avg, min, max
   * 
   * */
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
  }

  @Override
  public final Schema getResultSchema() {
    return resultSchema;
  }

}
