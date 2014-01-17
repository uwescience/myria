package edu.washington.escience.myria.operator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ConstantEvaluator;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;

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
  private TupleBatch state;

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
  public StatefulApply(final Operator child, final List<Expression> emitExpression,
      final List<Expression> initializerExpressions, final List<Expression> updaterExpressions) {
    super(child, emitExpression);
    // ToDo: make sure the init and update expressions have the same names
    if (initializerExpressions != null) {
      setInitExpressions(initializerExpressions);
    }
    if (updaterExpressions != null) {
      setUpdateExpressions(updaterExpressions);
    }
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
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkArgument(initExpressions.size() == updateExpressions.size());
    Preconditions.checkNotNull(getEmitExpressions());

    Schema inputSchema = getChild().getSchema();

    ArrayList<GenericEvaluator> evaluators = new ArrayList<>();
    evaluators.ensureCapacity(getEmitExpressions().size());
    setEvaluators(evaluators);
    for (Expression expr : getEmitExpressions()) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema, getStateSchema());
      if (evaluator.needsCompiling()) {
        evaluator.compile();
      }
      evaluators.add(evaluator);
    }

    setResultBuffer(new TupleBatchBuffer(getSchema()));

    // states = new TupleBatch(stateSchema, null, 1);

    updateEvaluators = new ArrayList<>();
    updateEvaluators.ensureCapacity(updateExpressions.size());

    TupleBatchBuffer tbb = new TupleBatchBuffer(getStateSchema());

    for (int columnIdx = 0; columnIdx < getStateSchema().numColumns(); columnIdx++) {
      Expression expr = initExpressions.get(columnIdx);
      ConstantEvaluator evaluator = new ConstantEvaluator(expr, inputSchema, getStateSchema());
      evaluator.compile();

      try {
        tbb.put(columnIdx, evaluator.eval());
      } catch (InvocationTargetException e) {
        throw new DbException(e);
      }
      state = tbb.popAny();
    }

    for (Expression expr : updateExpressions) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema, getStateSchema());
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
      typesBuilder.add(expr.getOutputType(getSchema(), null));
      namesBuilder.add(expr.getOutputName());
    }
    stateSchema = new Schema(typesBuilder.build(), namesBuilder.build());
    return stateSchema;
  }

  @Override
  protected void fillBuffer(final TupleBatch tb) throws DbException {
    for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
      TupleBatchBuffer tbb = new TupleBatchBuffer(getStateSchema());
      for (int columnIdx = 0; columnIdx < getStateSchema().numColumns(); columnIdx++) {
        try {
          tbb.put(columnIdx, updateEvaluators.get(columnIdx).eval(tb, rowIdx, state));
        } catch (InvocationTargetException e) {
          throw new DbException(e);
        }
      }
      state = tbb.popAny();
      for (int columnIdx = 0; columnIdx < getSchema().numColumns(); columnIdx++) {
        try {
          getEvaluator(columnIdx).evalAndPut(tb, rowIdx, getResultBuffer(), columnIdx, state);
        } catch (InvocationTargetException e) {
          throw new DbException(e);
        }
      }
    }
  }
}
