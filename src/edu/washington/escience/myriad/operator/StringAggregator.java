package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.parallel.ParallelUtility;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  /**
   * java Serialization id.
   * */
  private static final long serialVersionUID = 1L;

  private final int afield;
  private final int aggOps;
  private int count;
  private String min;
  private final boolean computeMin;
  private String max;
  private final boolean computeMax;
  private final Schema resultSchema;

  public static int AVAILABLE_AGG = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_MAX | Aggregator.AGG_OP_MIN;

  @Override
  public int available() {
    return AVAILABLE_AGG;
  }

  public StringAggregator(int afield, String aFieldName, int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException(
          "Unsupported aggregation on string column. Only count, min and max are supported");
    }

    this.afield = afield;
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
    if ((aggOps & Aggregator.AGG_OP_MIN) != 0) {
      computeMin = true;
      types[idx] = Type.STRING_TYPE;
      names[idx] = "min(" + aFieldName + ")";
      idx += 1;
    } else {
      computeMin = false;
    }
    if ((aggOps & Aggregator.AGG_OP_MAX) != 0) {
      types[idx] = Type.STRING_TYPE;
      names[idx] = "max(" + aFieldName + ")";
      idx += 1;
      computeMax = true;
    } else {
      computeMax = false;
    }
    resultSchema = new Schema(types, names);
  }

  @Override
  public void add(_TupleBatch tup) {

    count += tup.numOutputTuples();
    if (computeMin || computeMax) {
      StringColumn c = (StringColumn) tup.outputRawData().get(afield);
      int numTuples = c.size();
      for (int i = 0; i < numTuples; i++) {
        String r = c.getString(i);
        if (computeMin) {
          if (r.compareTo(min) < 0) {
            min = r;
          }
        }
        if (computeMax) {
          if (r.compareTo(max) > 0) {
            max = r;
          }
        }
      }
    }

  }

  @Override
  public void getResult(TupleBatchBuffer buffer, final int fromIndex) {
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
