package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregate over a StringColumn.
 */
public final class StringAggregator implements Aggregator<String> {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

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
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public StringAggregator(final String aFieldName, final int aggOps) {
    if (aggOps <= 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    if ((aggOps | AVAILABLE_AGG) != AVAILABLE_AGG) {
      throw new IllegalArgumentException(
          "Unsupported aggregation on string column. Only count, min and max are supported");
    }

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
  public void add(final ReadableTable from, final int fromColumn) {
    add(from.asColumn(fromColumn));
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
  public void getResult(final AppendableTable dest, final int destColumn) {
    int idx = destColumn;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      dest.putLong(idx, count);
      idx++;
    }
    if (computeMin) {
      dest.putString(idx, min);
      idx++;
    }
    if (computeMax) {
      dest.putString(idx, max);
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public void add(final ReadableTable table, final int column, final int row) {
    add(table.getString(column, row));
  }

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public void add(final ReadableColumn from) {
    final int numTuples = from.size();
    if (numTuples == 0) {
      return;
    }
    count += numTuples;
    if (computeMin || computeMax) {
      for (int i = 0; i < numTuples; i++) {
        final String r = from.getString(i);
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