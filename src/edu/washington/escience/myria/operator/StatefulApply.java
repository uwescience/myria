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
  private ImmutableList<Expression> initializerExpressions;

  /**
   * Expressions that are used to update the state.
   */
  private ImmutableList<Expression> updaterExpressions;

  /**
   * The states that are passed during execution.
   */
  private ArrayList<Object> states;

  /**
   * Evaluators that update the {@link #states}.
   */
  private ArrayList<GenericEvaluator> updateEvaluators;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param expressions expressions that creates the output
   * @param initializerExpressions expressions that initializes the state
   * @param updaterExpressions expressions that update the state
   */
  public StatefulApply(final Operator child, final List<Expression> expressions,
      final List<Expression> initializerExpressions, final List<Expression> updaterExpressions) {
    super(child, expressions);
    if (initializerExpressions != null) {
      setInitializers(initializerExpressions);
    }
    if (updaterExpressions != null) {
      setUpdaters(updaterExpressions);
    }
  }

  /**
   * 
   * @param initializerExpressions the expressions that initialize the state
   */
  private void setInitializers(final List<Expression> initializerExpressions) {
    this.initializerExpressions = ImmutableList.copyOf(initializerExpressions);
  }

  /**
   * 
   * @param updaterExpressions the expressions that update the state
   */
  private void setUpdaters(final List<Expression> updaterExpressions) {
    this.updaterExpressions = ImmutableList.copyOf(updaterExpressions);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkArgument(initializerExpressions.size() == getExpressions().size());
    super.init(execEnvVars);

    Schema inputSchema = getChild().getSchema();

    states = new ArrayList<>();
    states.ensureCapacity(initializerExpressions.size());

    updateEvaluators = new ArrayList<>();
    updateEvaluators.ensureCapacity(updaterExpressions.size());

    for (Expression expr : initializerExpressions) {
      ConstantEvaluator evaluator = new ConstantEvaluator(expr);
      evaluator.compile();

      try {
        states.add(evaluator.eval());
      } catch (InvocationTargetException e) {
        throw new DbException(e);
      }
    }

    for (Expression expr : updaterExpressions) {
      GenericEvaluator evaluator = new GenericEvaluator(expr, inputSchema);
      evaluator.compile();
      updateEvaluators.add(evaluator);
    }
  }

  /**
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
