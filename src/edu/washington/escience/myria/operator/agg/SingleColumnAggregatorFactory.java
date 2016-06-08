package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;

/**
 * An aggregator for a column of primitive type.
 */
public class SingleColumnAggregatorFactory implements AggregatorFactory {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input to aggregate over. */
  @JsonProperty private final int column;
  /** Which aggregate options are requested. See {@link PrimitiveAggregator}. */
  @JsonProperty private final AggregationOp[] aggOps;

  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   *
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  @JsonCreator
  public SingleColumnAggregatorFactory(
      @JsonProperty(value = "column", required = true) final Integer column,
      @JsonProperty(value = "aggOps", required = true) final AggregationOp... aggOps) {
    this.column = Objects.requireNonNull(column, "column").intValue();
    this.aggOps = Objects.requireNonNull(aggOps, "aggOps");
    Preconditions.checkArgument(aggOps.length > 0, "no aggregation operators selected");
    for (int i = 0; i < aggOps.length; ++i) {
      Preconditions.checkNotNull(aggOps[i], "aggregation operator %s cannot be null", i);
    }
  }

  @Override
  public Aggregator get(final Schema inputSchema) {
    Objects.requireNonNull(inputSchema, "inputSchema");
    Objects.requireNonNull(aggOps, "aggOps");
    String inputName = inputSchema.getColumnName(column);
    Type type = inputSchema.getColumnType(column);
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanAggregator(inputName, aggOps, column);
      case DATETIME_TYPE:
        return new DateTimeAggregator(inputName, aggOps, column);
      case DOUBLE_TYPE:
        return new DoubleAggregator(inputName, aggOps, column);
      case FLOAT_TYPE:
        return new FloatAggregator(inputName, aggOps, column);
      case INT_TYPE:
        return new IntegerAggregator(inputName, aggOps, column);
      case LONG_TYPE:
        return new LongAggregator(inputName, aggOps, column);
      case STRING_TYPE:
        return new StringAggregator(inputName, aggOps, column);
    }
    throw new IllegalArgumentException("Unknown column type: " + type);
  }
}
