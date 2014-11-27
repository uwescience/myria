package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.Tuple;

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
   * The states that are passed during execution.
   */
  private transient Tuple state;
  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updaters}.
   */
  private transient ArrayList<GenericEvaluator> updateEvaluators;
  /**
   * One evaluator for each expression in {@link #emitters}.
   */
  private transient ArrayList<GenericEvaluator> emitEvaluators;

  /**
   * The schema of the result tuples.
   */
  private transient Schema resultSchema;

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
    state = null;
    updateEvaluators = null;
    emitEvaluators = null;
    resultSchema = null;
  }

  @Override
  @Nonnull
  public Aggregator get(final Schema inputSchema) throws DbException {
    if (state == null) {
      Objects.requireNonNull(inputSchema, "inputSchema");
      Preconditions.checkArgument(initializers.size() == updaters.size(),
          "must have the same number of aggregate state initializers (%s) and updaters (%s)", initializers.size(),
          updaters.size());
      // Verify that initializers and updaters have compatible names
      for (int i = 0; i < initializers.size(); i++) {
        Preconditions.checkArgument(Objects
            .equals(initializers.get(i).getOutputName(), updaters.get(i).getOutputName()),
            "initializers[i] and updaters[i] have different names (%s) != (%s)", initializers.get(i).getOutputName(),
            updaters.get(i).getOutputName());
      }

      /* Initialize the state. */
      Schema stateSchema = generateStateSchema(inputSchema);
      state = new Tuple(stateSchema);
      for (int columnIdx = 0; columnIdx < stateSchema.numColumns(); columnIdx++) {
        Expression expr = initializers.get(columnIdx);
        ConstantEvaluator evaluator = new ConstantEvaluator(expr, new ExpressionOperatorParameter());
        evaluator.compile();
        state.set(columnIdx, evaluator.eval());
      }

      /* Set up the updaters. */
      updateEvaluators = new ArrayList<>(updaters.size());
      for (Expression expr : updaters) {
        GenericEvaluator evaluator =
            new GenericEvaluator(expr, new ExpressionOperatorParameter(inputSchema, stateSchema));
        evaluator.compile();
        updateEvaluators.add(evaluator);
      }

      /* Set up the emitters. */
      emitEvaluators = new ArrayList<>();
      emitEvaluators.ensureCapacity(emitters.size());
      for (Expression expr : emitters) {
        GenericEvaluator evaluator = new GenericEvaluator(expr, new ExpressionOperatorParameter(null, stateSchema));
        evaluator.compile();
        emitEvaluators.add(evaluator);
      }

      /* Compute the result schema. */
      ExpressionOperatorParameter emitParams = new ExpressionOperatorParameter(null, stateSchema);
      ImmutableList.Builder<Type> types = ImmutableList.builder();
      ImmutableList.Builder<String> names = ImmutableList.builder();
      for (Expression e : emitters) {
        types.add(e.getOutputType(emitParams));
        names.add(e.getOutputName());
      }
      resultSchema = new Schema(types, names);
    }
    return new UserDefinedAggregator(state.clone(), updateEvaluators, emitEvaluators, resultSchema);
  }

  /**
   * Generate the schema of the state.
   * 
   * @param inputSchema the {@link Schema} of the input tuples.
   * @return the {@link Schema} of the state assuming the specified input types.
   */
  private Schema generateStateSchema(final Schema inputSchema) {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : initializers) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
