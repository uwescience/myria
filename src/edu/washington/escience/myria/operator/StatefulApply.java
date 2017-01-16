package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class StatefulApply extends Apply {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Expressions that are used to initialize the state.
   */
  private ImmutableList<Expression> initExpressions;

  /**
   * Expressions that are used to update the state.
   */
  private ImmutableList<Expression> updateExpressions;

  /**
   * The states that are passed during execution.
   */
  private Tuple state;

  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}.
   */
  private ArrayList<GenericEvaluator> updateEvaluators;

  /**
   * Schema of the state relation.
   */
  private Schema stateSchema = null;

  /**
   * @param child child operator that data is fetched from
   * @param emitExpression expressions that creates the output
   * @param initializerExpressions expressions that initializes the state
   * @param updaterExpressions expressions that update the state
   */
  public StatefulApply(
      final Operator child,
      final List<Expression> emitExpression,
      final List<Expression> initializerExpressions,
      final List<Expression> updaterExpressions) {
    super(child, emitExpression);
    Preconditions.checkArgument(initializerExpressions.size() == updaterExpressions.size());
    for (int i = 0; i < initializerExpressions.size(); i++) {
      Preconditions.checkArgument(
          updaterExpressions.get(i).getOutputName() == null
              || initializerExpressions
                  .get(i)
                  .getOutputName()
                  .equals(updaterExpressions.get(i).getOutputName()));
    }
    setInitExpressions(initializerExpressions);
    setUpdateExpressions(updaterExpressions);
  }

  /**
   * @param initializerExpressions the expressions that initialize the state
   */
  private void setInitExpressions(final List<Expression> initializerExpressions) {
    initExpressions = ImmutableList.copyOf(initializerExpressions);
  }

  /**
   * @param updaterExpressions the expressions that update the state
   */
  private void setUpdateExpressions(final List<Expression> updaterExpressions) {
    updateExpressions = ImmutableList.copyOf(updaterExpressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, InvocationTargetException {
    Operator child = getChild();

    if (child.eoi() || getChild().eos()) {
      return null;
    }

    TupleBatch tb = child.nextReady();
    if (tb == null) {
      return null;
    }

    final int numColumns = getSchema().numColumns();

    List<Column<?>> output = Lists.newArrayList(new Column<?>[numColumns]);
    List<Integer> needState = Lists.newLinkedList();

    // first, generate columns that do not require state. This can often be optimized.
    for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
      final GenericEvaluator evaluator = getEmitEvaluators().get(columnIdx);
      Preconditions.checkArgument(
          !evaluator.getExpression().isMultivalued(),
          "A multivalued expression cannot be used in StatefulApply.");
      if (!evaluator.needsState() || evaluator.isCopyFromInput()) {
        output.set(columnIdx, evaluator.evaluateColumn(tb).getResultColumns().get(0));
      } else {
        needState.add(columnIdx);
      }
    }

    // second, build and add the columns that require state
    List<ColumnBuilder<?>> columnBuilders = Lists.newArrayListWithCapacity(needState.size());
    for (int builderIdx = 0; builderIdx < needState.size(); builderIdx++) {
      columnBuilders.add(
          ColumnFactory.allocateColumn(
              getEmitEvaluators().get(needState.get(builderIdx)).getOutputType()));
    }

    for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
      // update state
      Tuple newState = new Tuple(getStateSchema());
      for (int columnIdx = 0; columnIdx < stateSchema.numColumns(); columnIdx++) {
        updateEvaluators
            .get(columnIdx)
            .eval(tb, rowIdx, null, newState.getColumn(columnIdx), state);
      }
      state = newState;
      // apply expression
      for (int index = 0; index < needState.size(); index++) {
        final GenericEvaluator evaluator = getEmitEvaluators().get(needState.get(index));
        // TODO: optimize the case where the state is copied directly
        evaluator.eval(tb, rowIdx, null, columnBuilders.get(index), state);
      }
    }

    for (int builderIdx = 0; builderIdx < needState.size(); builderIdx++) {
      output.set(needState.get(builderIdx), columnBuilders.get(builderIdx).build());
    }

    return new TupleBatch(getSchema(), output);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkArgument(initExpressions.size() == updateExpressions.size());
    Preconditions.checkNotNull(getEmitExpressions());

    final Schema inputSchema = getChild().getSchema();

    ArrayList<GenericEvaluator> evaluators = new ArrayList<>();
    evaluators.ensureCapacity(getEmitExpressions().size());
    for (Expression expr : getEmitExpressions()) {
      GenericEvaluator evaluator;
      if (expr.isConstant()) {
        evaluator =
            new ConstantEvaluator(expr, new ExpressionOperatorParameter(inputSchema, getNodeID()));
      } else {
        evaluator =
            new GenericEvaluator(
                expr, new ExpressionOperatorParameter(inputSchema, getStateSchema(), getNodeID()));
      }
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      evaluators.add(evaluator);
    }
    setEmitEvaluators(evaluators);

    updateEvaluators = new ArrayList<>();
    updateEvaluators.ensureCapacity(updateExpressions.size());

    state = new Tuple(getStateSchema());

    for (int columnIdx = 0; columnIdx < getStateSchema().numColumns(); columnIdx++) {
      Expression expr = initExpressions.get(columnIdx);
      ConstantEvaluator evaluator =
          new ConstantEvaluator(expr, new ExpressionOperatorParameter(inputSchema, getNodeID()));
      evaluator.compile();
      state.set(columnIdx, evaluator.eval());
    }

    for (Expression expr : updateExpressions) {
      GenericEvaluator evaluator =
          new GenericEvaluator(
              expr, new ExpressionOperatorParameter(inputSchema, getStateSchema(), getNodeID()));
      evaluator.compile();
      updateEvaluators.add(evaluator);
    }
  }

  /**
   * @return The schema of the state relation.
   */
  private Schema getStateSchema() {
    if (stateSchema == null) {
      return generateStateSchema();
    }
    return stateSchema;
  }

  /**
   * Generates the state schema and returns it.
   *
   * @return the state schema
   */
  private Schema generateStateSchema() {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : initExpressions) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(getChild().getSchema())));
      namesBuilder.add(expr.getOutputName());
    }
    stateSchema = new Schema(typesBuilder.build(), namesBuilder.build());
    return stateSchema;
  }

  @Override
  public Schema generateSchema() {
    if (getEmitExpressions() == null) {
      return null;
    }
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema inputSchema = child.getSchema();
    if (inputSchema == null) {
      return null;
    }
    if (getStateSchema() == null) {
      return null;
    }

    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : getEmitExpressions()) {
      typesBuilder.add(
          expr.getOutputType(new ExpressionOperatorParameter(inputSchema, getStateSchema())));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
