package edu.washington.escience.myria.operator.agg;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
  @JsonProperty
  private final int column;
  /** Which aggregate options are requested. See {@link PrimitiveAggregator}. */
  @JsonProperty
  private final List<String> aggOps;
  /** The actual aggregator doing the work. */
  private PrimitiveAggregator agg;

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   * 
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  @JsonCreator
  public SingleColumnAggregator(@JsonProperty(value = "column", required = true) final Integer column,
      @JsonProperty(value = "aggOps", required = true) final List<String> aggOps) {
    this.column = Objects.requireNonNull(column, "column").intValue();
    this.aggOps = Objects.requireNonNull(aggOps, "aggOps");
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
  public Schema getResultSchema(final Schema childSchema) {
    if (agg == null) {
      agg =
          AggUtils.allocate(childSchema.getColumnType(column), childSchema.getColumnName(column), AggregateEncoding
              .deserializeAggregateOperators(aggOps));
    }
    return agg.getResultSchema();
  }
}
