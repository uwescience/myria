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

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.DivideExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
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
  public List<Aggregator> generateInternalAggs(final Schema inputSchema) {
    List<Aggregator> ret = new ArrayList<Aggregator>();
    List<AggregationOp> ops = getInternalOps();
    for (int i = 0; i < ops.size(); ++i) {
      ret.add(generateAgg(inputSchema, ops.get(i)));
    }
    return ret;
  }

  /**
   * @param inputSchema the input schema
   * @param aggOp the aggregation op
   * @param indices the column indices of this aggregator in the state hash table
   * @return the generated aggregator
   */
  private Aggregator generateAgg(final Schema inputSchema, final AggregationOp aggOp) {
    String inputName = inputSchema.getColumnName(column);
    Type type = inputSchema.getColumnType(column);
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanAggregator(inputName, column, aggOp);
      case DATETIME_TYPE:
        return new DateTimeAggregator(inputName, column, aggOp);
      case DOUBLE_TYPE:
        return new DoubleAggregator(inputName, column, aggOp);
      case FLOAT_TYPE:
        return new FloatAggregator(inputName, column, aggOp);
      case INT_TYPE:
        return new IntegerAggregator(inputName, column, aggOp);
      case LONG_TYPE:
        return new LongAggregator(inputName, column, aggOp);
      case STRING_TYPE:
        return new StringAggregator(inputName, column, aggOp);
      default:
        throw new IllegalArgumentException("Unknown column type: " + type);
    }
  }

  @Override
  public List<Expression> generateEmitExpressions(final Schema inputSchema) {
    List<AggregationOp> cols = getInternalOps();
    List<Expression> exps = new ArrayList<Expression>();
    for (int i = 0; i < aggOps.length; ++i) {
      String name = aggOps[i].toString().toLowerCase() + "_" + inputSchema.getColumnName(column);
      switch (aggOps[i]) {
        case COUNT:
        case MIN:
        case MAX:
        case SUM:
          exps.add(new Expression(name, new VariableExpression(cols.indexOf(aggOps[i]))));
          continue;
        case AVG:
          exps.add(
              new Expression(
                  name,
                  new DivideExpression(
                      new VariableExpression(cols.indexOf(AggregationOp.SUM)),
                      new VariableExpression(cols.indexOf(AggregationOp.COUNT)))));
          continue;
        case STDEV:
          ExpressionOperator sumExp = new VariableExpression(cols.indexOf(AggregationOp.SUM));
          ExpressionOperator countExp = new VariableExpression(cols.indexOf(AggregationOp.COUNT));
          ExpressionOperator sumSquaredExp =
              new VariableExpression(cols.indexOf(AggregationOp.SUM_SQUARED));
          ExpressionOperator first = new DivideExpression(sumSquaredExp, countExp);
          ExpressionOperator second = new DivideExpression(sumExp, countExp);
          exps.add(
              new Expression(
                  name,
                  new SqrtExpression(
                      new MinusExpression(first, new TimesExpression(second, second)))));
          continue;
        default:
          throw new IllegalArgumentException("Type " + aggOps[i] + " is invalid");
      }
    }
    return exps;
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
      case MIN:
      case MAX:
      case SUM:
        return ImmutableList.of(op);
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
        throw new IllegalArgumentException("SUM_SQUARED on " + op + " is invalid");
      case AVG:
      case STDEV:
        return Type.DOUBLE_TYPE;
      default:
        throw new IllegalArgumentException(op + " is invalid");
    }
  }

  @Override
  public Schema generateSchema(final Schema inputSchema) {
    List<String> names = new ArrayList<String>();
    List<Type> types = new ArrayList<Type>();
    for (AggregationOp op : aggOps) {
      types.add(getAggColumnType(inputSchema.getColumnType(column), op));
      names.add(op.toString().toLowerCase() + "_" + inputSchema.getColumnName(column));
    }
    return Schema.of(types, names);
  }

  @Override
  public Schema generateStateSchema(final Schema inputSchema) {
    List<String> names = new ArrayList<String>();
    List<Type> types = new ArrayList<Type>();
    for (AggregationOp op : getInternalOps()) {
      types.add(getAggColumnType(inputSchema.getColumnType(column), op));
      names.add(op.toString().toLowerCase() + "_" + inputSchema.getColumnName(column));
    }
    return Schema.of(types, names);
  }

  PythonFunctionRegistrar pyFuncReg;

  public void setPyFuncReg(PythonFunctionRegistrar pyFuncReg) {
    this.pyFuncReg = pyFuncReg;
  }
}
