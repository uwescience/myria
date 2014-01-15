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
import edu.washington.escience.myria.expression.ConstantEvaluator;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.GenericEvaluator;

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
  private ArrayList<Object> states;

  /**
   * Evaluators that update the {@link #states}. One evaluator for each expression in {@link #updateExpressions}.
   */
  private ArrayList<GenericEvaluator> updateEvaluators;

  /**
   * @param child child operator that data is fetched from
   * @param emitExpression expressions that creates the output
   * @param initializerExpressions expressions that initializes the state
   * @param updaterExpressions expressions that update the state
   */
  public StatefulApply(final Operator child, final List<Expression> emitExpression,
      final List<Expression> initializerExpressions, final List<Expression> updaterExpressions) {
    super(child, emitExpression);
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
    Preconditions.checkArgument(initExpressions.size() == getEmitExpressions().size());
    super.init(execEnvVars);

    Schema inputSchema = getChild().getSchema();

    states = new ArrayList<>();
    states.ensureCapacity(initExpressions.size());

    updateEvaluators = new ArrayList<>();
    updateEvaluators.ensureCapacity(updateExpressions.size());

    for (Expression expr : initExpressions) {
      ConstantEvaluator evaluator = new ConstantEvaluator(expr);
      evaluator.compile();

      try {
        states.add(evaluator.eval());
      } catch (InvocationTargetException e) {
        throw new DbException(e);
      }
    }

    for (Expression expr : updateExpressions) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema);
      evaluator.compile();
      updateEvaluators.add(evaluator);
    }
  }

  /**
   * Overrides function call in {@link #fetchNextReady()}.
   * 
   * @param tb the source tuple batch
   * @param rowIdx the current row index
   * @param columnIdx the current column index
   * @throws InvocationTargetException exception when evaluating
   */
  @Override
  protected void evaluate(final TupleBatch tb, final int rowIdx, final int columnIdx) throws InvocationTargetException {
    final Object state = states.get(columnIdx);

    getEvaluator(columnIdx).evalAndPut(tb, rowIdx, getResultBuffer(), columnIdx, state);
    states.set(columnIdx, updateEvaluators.get(columnIdx).eval(tb, rowIdx, state));
  }
}
