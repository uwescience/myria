package edu.washington.escience.myria.operator.agg;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregator implements Aggregator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Expressions that are used to initialize the state.
   */
  private final ImmutableList<Expression> initializers;

  /**
   * List of expressions that will be used to create the output.
   */
  private final ImmutableList<Expression> emitters;

  /**
   * The schema of the tuples that this aggregator will aggregate.
   */
  private final Schema inputSchema;

  /**
   * One evaluator for each expression in {@link #emitExpressions}.
   */
  private final ArrayList<GenericEvaluator> emitEvaluators;
  /**
   * The states that are passed during execution.
   */
  private Tuple state;

  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}.
   */
  private final ArrayList<GenericEvaluator> updateEvaluators;

  /**
   * Schema of the state relation.
   */
  private Schema stateSchema = null;

  /**
   * @param initializers expressions that initializes the state
   * @param updaters expressions that update the state
   * @param emitters expressions that creates the output
   * @param inputSchema child operator that data is fetched from
   * @throws DbException if there is an error initializing the evaluators.
   */
  public UserDefinedAggregator(final List<Expression> initializers, final List<Expression> updaters,
      final List<Expression> emitters, final Schema inputSchema) throws DbException {
    this.initializers = ImmutableList.copyOf(MyriaUtils.checkHasNoNulls(initializers, "initializers"));
    ImmutableList.copyOf(MyriaUtils.checkHasNoNulls(updaters, "updaters"));
    this.emitters = ImmutableList.copyOf(MyriaUtils.checkHasNoNulls(emitters, "emitters"));
    this.inputSchema = Objects.requireNonNull(inputSchema, "inputSchema");

    Objects.requireNonNull(inputSchema, "inputSchema");
    Preconditions.checkArgument(initializers.size() == updaters.size(),
        "must have the same number of aggregate state initializers (%s) and updaters (%s)", initializers.size(),
        updaters.size());
    // Verify that initializers and updaters have compatible names
    for (int i = 0; i < initializers.size(); i++) {
      Preconditions.checkArgument(Objects.equals(initializers.get(i).getOutputName(), updaters.get(i).getOutputName()),
          "initializers[i] and updaters[i] have different names (%s) != (%s)", initializers.get(i).getOutputName(),
          updaters.get(i).getOutputName());
    }

    /* Initialize the state. */
    generateStateSchema();
    state = new Tuple(stateSchema);
    for (int columnIdx = 0; columnIdx < stateSchema.numColumns(); columnIdx++) {
      Expression expr = initializers.get(columnIdx);
      ConstantEvaluator evaluator = new ConstantEvaluator(expr, new ExpressionOperatorParameter(inputSchema));
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
  }

  /**
   * Generate the schema of the state.
   */
  private void generateStateSchema() {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : initializers) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    stateSchema = new Schema(typesBuilder.build(), namesBuilder.build());
  }

  @Override
  public void add(final ReadableTable from) throws DbException {
    for (int row = 0; row < from.numTuples(); ++row) {
      addRow(from, row);
    }
  }

  @Override
  public void addRow(final ReadableTable from, final int row) throws DbException {
    // update state
    Tuple newState = new Tuple(stateSchema);
    for (int columnIdx = 0; columnIdx < stateSchema.numColumns(); columnIdx++) {
      try {
        updateEvaluators.get(columnIdx).eval(from, row, newState.getColumn(columnIdx), state);
      } catch (InvocationTargetException e) {
        throw new DbException("Error updating state", e);
      }
    }
    state = newState;
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) throws DbException {
    for (int index = 0; index < emitters.size(); index++) {
      final GenericEvaluator evaluator = emitEvaluators.get(index);
      // TODO: optimize the case where the state is copied directly
      try {
        evaluator.eval(null, 0, dest.asWritableColumn(destColumn + index), state);
      } catch (InvocationTargetException e) {
        throw new DbException("Error finalizing aggregate", e);
      }
    }
  }

  @Override
  public Schema getResultSchema() {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : emitters) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(null, stateSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
