package edu.washington.escience.myria.operator.agg;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;

/**
 * An aggregator for a column of primitive type.
 */
public class SingleColumnAggregatorFactory implements AggregatorFactory {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input to aggregate over. */
  @JsonProperty
  private final int column;
  /** Which aggregate options are requested. See {@link PrimitiveAggregator}. */
  @JsonProperty
  private final List<String> aggOps;

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   * 
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  @JsonCreator
  public SingleColumnAggregatorFactory(@JsonProperty(value = "column", required = true) final Integer column,
      @JsonProperty(value = "aggOps", required = true) final List<String> aggOps) {
    this.column = Objects.requireNonNull(column, "column").intValue();
    this.aggOps = Objects.requireNonNull(aggOps, "aggOps");
  }

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   * 
   * @param column which column of the input to aggregate over.
   * @param aggOp the aggregate operations that is requested. See {@link PrimitiveAggregator}.
   */
  public SingleColumnAggregatorFactory(final int column, @Nonnull final String aggOp) {
    this.column = column;
    aggOps = ImmutableList.of(Objects.requireNonNull(aggOp, "aggOp"));
  }

  @Override
  public Aggregator get(@Nonnull final Schema inputSchema) {
    return new SingleColumnAggregator(inputSchema, column, aggOps);
  }
}
