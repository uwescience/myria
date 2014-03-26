package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator implements Aggregator<Boolean> {

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
   * @param aFieldName aggregate field name for use in output schema.
   * @param aggOps the aggregate operation to simultaneously compute.
   * */
  public BooleanAggregator(final String aFieldName, final int aggOps) {
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
  public void add(final ReadableTable from, final int fromColumn) {
    count += from.numTuples();
  }

  /**
   * Add the specified value to this aggregator.
   * 
   * @param value the value to be added
   */
  public void addBoolean(final boolean value) {
    count++;
  }

  @Override
  public void add(final Boolean value) {
    addBoolean(Objects.requireNonNull(value, "value"));
  }

  @Override
  public void addObj(final Object obj) {
    this.add((Boolean) obj);
  }

  @Override
  public int availableAgg() {
    return AVAILABLE_AGG;
  }

  @Override
  public void getResult(final AppendableTable outputBuffer, final int destColumn) {
    int idx = destColumn;
    if ((aggOps & AGG_OP_COUNT) != 0) {
      outputBuffer.putLong(idx, count);
      idx++;
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public void add(final ReadableTable t, final int column, final int row) {
    add(t.getBoolean(column, row));
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public void add(final ReadableColumn from) {
    count += from.size();
  }
}
