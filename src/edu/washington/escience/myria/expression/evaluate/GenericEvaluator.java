package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.ReadableTable;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply} and {@link StatefulApply}.
 */
public class GenericEvaluator extends Evaluator {

  /**
   * logger for this class.
   * */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * Expression evaluator.
   */
  private EvalInterface evaluator;

  /**
   * True if the expression uses state.
   */
  private final boolean needsState;

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param inputSchema the schema that the expression expects
   * @param stateSchema the schema of the state
   */
  public GenericEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
    needsState = getExpression().hasOperator(StateExpression.class);
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    Preconditions.checkArgument(needsCompiling(), "This expression does not need to be compiled.");

    String javaExpression = "";

    try {
      javaExpression = getJavaExpression();
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator =
          (EvalInterface) se.createFastEvaluator(javaExpression, EvalInterface.class, new String[] {
              Expression.TB, Expression.ROW, Expression.RESULT, Expression.STATE });
    } catch (Exception e) {
      LOGGER.debug("Expression: %s", javaExpression);
      throw new DbException("Error when compiling expression", e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpression()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} as it can copy data without evaluating the expression.
   * 
   * @param tb a tuple batch
   * @param rowIdx the row that should be used for input data
   * @param result the column that the result should be appended to
   * @param state additional state that affects the computation
   * @throws InvocationTargetException exception thrown from janino
   */
  public void eval(final TupleBatch tb, final int rowIdx, final WritableColumn result, final ReadableTable state)
      throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    evaluator.evaluate(tb, rowIdx, result, state);
  }

  /**
   * @return the Java form of this expression.
   */
  @Override
  public String getJavaExpression() {
    return getExpression().getJavaExpressionWithAppend(getInputSchema(), getStateSchema());
  }

  /**
   * Evaluate an expression over an entire TupleBatch and return the column of results. This method cannot take state
   * into consideration.
   * 
   * @param tb the tuples to be input to this expression
   * @return a column containing the result of evaluating this expression on the entire TupleBatch
   * @throws InvocationTargetException exception thrown from janino
   */
  public Column<?> evaluateColumn(final TupleBatch tb) throws InvocationTargetException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    /* This expression just copies an input column. */
    if (isCopyFromInput()) {
      return tb.getDataColumns().get(((VariableExpression) op).getColumnIdx());
    }

    Type type = getOutputType();

    ColumnBuilder<?> ret = ColumnFactory.allocateColumn(type);
    for (int row = 0; row < tb.numTuples(); ++row) {
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      eval(tb, row, ret, null);
    }
    return ret.build();
  }

  /**
   * @return true if the expression accesses the state.
   */
  public boolean needsState() {
    return needsState;
  }
}
