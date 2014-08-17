package edu.washington.escience.myria.operator.agg;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregatorFactory implements AggregatorFactory {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Expressions that initialize the state variables. */
  @JsonProperty
  private final List<Expression> initializers;
  /** Expressions that update the state variables as a function of the input and the current tuple. */
  @JsonProperty
  private final List<Expression> updaters;
  /** Expressions that emit the final aggregation result from the state. */
  @JsonProperty
  private final List<Expression> emitters;

  /**
   * Construct a new user-defined aggregate. The initializers set the initial state of the aggregate; the updaters
   * update this state for every new tuple. The emitters produce the final value of the aggregate. Note that there must
   * be the same number of initializers and updaters, but there may be any number > 0 of emitters.
   * 
   * @param initializers Expressions that initialize the state variables.
   * @param updaters Expressions that update the state variables as a function of the input and the current tuple.
   * @param emitters Expressions that emit the final aggregation result from the state.
   */
  @JsonCreator
  public UserDefinedAggregatorFactory(
      @JsonProperty(value = "initializers", required = true) final List<Expression> initializers,
      @JsonProperty(value = "updaters", required = true) final List<Expression> updaters,
      @JsonProperty(value = "emitters", required = true) final List<Expression> emitters) {
    this.initializers = Objects.requireNonNull(initializers, "initializers");
    this.updaters = Objects.requireNonNull(updaters, "updaters");
    this.emitters = Objects.requireNonNull(emitters, "emitters");
  }

  @Override
  @Nonnull
  public Aggregator get(final Schema inputSchema) throws DbException {
    return new UserDefinedAggregator(initializers, updaters, emitters, inputSchema);
  }

  @Override
  @Nonnull
  public Schema getResultSchema(final Schema inputSchema) {
    ImmutableList.Builder<Type> stateTypes = ImmutableList.builder();
    ExpressionOperatorParameter initParams = new ExpressionOperatorParameter(inputSchema);
    for (Expression e : initializers) {
      stateTypes.add(e.getOutputType(initParams));
    }
    Schema stateSchema = new Schema(stateTypes.build());

    ExpressionOperatorParameter emitParams = new ExpressionOperatorParameter(null, stateSchema);
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (Expression e : emitters) {
      types.add(e.getOutputType(emitParams));
      names.add(e.getOutputName());
    }
    return new Schema(types, names);
  }
}
