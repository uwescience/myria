package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;

/**
 * A factory that generates aggregators for a primitive column.
 */
public class PrimitiveAggregatorFactory implements AggregatorFactory {

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
  public PrimitiveAggregatorFactory(
      @JsonProperty(value = "column", required = true) final Integer column,
      @JsonProperty(value = "aggOps", required = true) final AggregationOp[] aggOps) {
    this.column = Objects.requireNonNull(column, "column").intValue();
    this.aggOps = Objects.requireNonNull(aggOps, "aggOps");
    Preconditions.checkNotNull(aggOps, "aggregation operator %s cannot be null");
  }

  /**
   * @param column which column of the input to aggregate over.
   * @param aggOp which aggregate is requested.
   */
  public PrimitiveAggregatorFactory(final Integer column, final AggregationOp aggOp) {
    this(column, new AggregationOp[] {aggOp});
  }

  @Override
  public List<Aggregator> generateInternalAggs(final Schema inputSchema, final int offset) {
    List<Aggregator> ret = new ArrayList<Aggregator>();
    List<AggregationOp> ops = getInternalOps();
    for (int i = 0; i < ops.size(); ++i) {
      switch (ops.get(i)) {
        case COUNT:
          ret.add(generateAgg(inputSchema, AggregationOp.COUNT, new int[] {offset + i}));
          break;
        case SUM:
          ret.add(generateAgg(inputSchema, AggregationOp.SUM, new int[] {offset + i}));
          break;
        case MIN:
          ret.add(generateAgg(inputSchema, AggregationOp.MIN, new int[] {offset + i}));
          break;
        case MAX:
          ret.add(generateAgg(inputSchema, AggregationOp.MAX, new int[] {offset + i}));
          break;
        case SUM_SQUARED:
          ret.add(generateAgg(inputSchema, AggregationOp.SUM_SQUARED, new int[] {offset + i}));
          break;
        default:
          throw new IllegalArgumentException("Invalid internal aggregate type: " + ops.get(i));
      }
    }
    return ret;
  }

  /**
   * @param inputSchema the input schema
   * @param aggOp the aggregation op
   * @param indices the column indices of this aggregator in the state hash table
   * @return the generated aggregator
   */
  private Aggregator generateAgg(
      final Schema inputSchema, final AggregationOp aggOp, final int[] indices) {
    String inputName = inputSchema.getColumnName(column);
    Type type = inputSchema.getColumnType(column);
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanAggregator(inputName, column, aggOp, indices);
      case DATETIME_TYPE:
        return new DateTimeAggregator(inputName, column, aggOp, indices);
      case DOUBLE_TYPE:
        return new DoubleAggregator(inputName, column, aggOp, indices);
      case FLOAT_TYPE:
        return new FloatAggregator(inputName, column, aggOp, indices);
      case INT_TYPE:
        return new IntegerAggregator(inputName, column, aggOp, indices);
      case LONG_TYPE:
        return new LongAggregator(inputName, column, aggOp, indices);
      case STRING_TYPE:
        return new StringAggregator(inputName, column, aggOp, indices);
      default:
        throw new IllegalArgumentException("Unknown column type: " + type);
    }
  }

  @Override
  public List<Aggregator> generateEmitAggs(final Schema inputSchema, final int offset) {
    List<Aggregator> aggs = new ArrayList<Aggregator>();
    List<AggregationOp> cols = getInternalOps();
    for (AggregationOp aggOp : aggOps) {
      List<Integer> indices = new ArrayList<Integer>();
      for (AggregationOp op : getInternalOps(aggOp)) {
        indices.add(cols.indexOf(op) + offset);
      }
      aggs.add(generateAgg(inputSchema, aggOp, Ints.toArray(indices)));
    }
    return aggs;
  }

  /**
   * Generate the internal aggregation ops. Each used op corresponds to one column.
   *
   * @return the list of aggregation ops.
   */
  public List<AggregationOp> getInternalOps() {
    Set<AggregationOp> colTypes = new HashSet<AggregationOp>();
    for (AggregationOp aggOp : aggOps) {
      colTypes.addAll(getInternalOps(aggOp));
    }
    List<AggregationOp> ret = new ArrayList<AggregationOp>(colTypes);
    Collections.sort(ret);
    return ret;
  }

  /**
   * @param op the emit aggregation op
   * @return the internal aggregation ops needed for computing the emit op
   */
  private List<AggregationOp> getInternalOps(AggregationOp op) {
    switch (op) {
      case COUNT:
        return ImmutableList.of(AggregationOp.COUNT);
      case MIN:
        return ImmutableList.of(AggregationOp.MIN);
      case MAX:
        return ImmutableList.of(AggregationOp.MAX);
      case SUM:
        return ImmutableList.of(AggregationOp.SUM);
      case AVG:
        return ImmutableList.of(AggregationOp.SUM, AggregationOp.COUNT);
      case STDEV:
        return ImmutableList.of(AggregationOp.SUM, AggregationOp.SUM_SQUARED, AggregationOp.COUNT);
      default:
        throw new IllegalArgumentException("Type " + op + " is invalid");
    }
  }

  /**
   * @param input the input type
   * @param op the aggregation op
   * @return the output type of applying op on the input type
   */
  public Type getAggColumnType(Type input, AggregationOp op) {
    switch (op) {
      case MIN:
      case MAX:
        return input;
      case COUNT:
        return Type.LONG_TYPE;
      case SUM:
      case SUM_SQUARED:
        if (input == Type.INT_TYPE || input == Type.LONG_TYPE) {
          return Type.LONG_TYPE;
        }
        if (input == Type.FLOAT_TYPE || input == Type.DOUBLE_TYPE) {
          return Type.DOUBLE_TYPE;
        }
      default:
        throw new IllegalArgumentException(op + " is invalid");
    }
  }

  @Override
  public Schema generateStateSchema(Schema inputSchema) {
    Schema schema = Schema.EMPTY_SCHEMA;
    for (AggregationOp op : getInternalOps()) {
      Type inputType = inputSchema.getColumnType(column);
      schema =
          Schema.merge(
              schema,
              Schema.ofFields(
                  getAggColumnType(inputType, op),
                  op.toString().toLowerCase() + "_" + inputSchema.getColumnName(column)));
    }
    return schema;
  }
}
