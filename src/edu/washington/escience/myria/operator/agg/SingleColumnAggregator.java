package edu.washington.escience.myria.operator.agg;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.AggregateEncoding;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator for a column of primitive type.
 */
public class SingleColumnAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input to aggregate over. */
  private final int column;
  /** The actual aggregator doing the work. */
  private final PrimitiveAggregator agg;

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   * 
   * @param inputSchema the schema of the input tuples.
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  public SingleColumnAggregator(@Nonnull final Schema inputSchema, final int column, @Nonnull final List<String> aggOps) {
    Objects.requireNonNull(inputSchema, "inputSchema");
    this.column = column;
    Objects.requireNonNull(aggOps, "aggOps");
    agg =
        AggUtils.allocate(inputSchema.getColumnType(column), inputSchema.getColumnName(column), AggregateEncoding
            .deserializeAggregateOperators(aggOps));
  }

  @Override
  public void add(final ReadableTable from) {
    Preconditions.checkState(agg != null, "agg should not be null. Did you call getResultSchema yet?");
    agg.add(from, column);
  }

  @Override
  public void addRow(final ReadableTable from, final int row) {
    Preconditions.checkState(agg != null, "agg should not be null. Did you call getResultSchema yet?");
    agg.add(from, column, row);
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) {
    Preconditions.checkState(agg != null, "agg should not be null. Did you call getResultSchema yet?");
    agg.getResult(dest, destColumn);
  }

  @Override
  public Schema getResultSchema() {
    return agg.getResultSchema();
  }
}
