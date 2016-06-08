package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An Expression evaluator for stateless boolean expressions.
 */
public class BooleanEvaluator extends Evaluator {
  /**
   * Expression evaluator.
   */
  private BooleanEvalInterface evaluator;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   */
  public BooleanEvaluator(
      final Expression expression, final ExpressionOperatorParameter parameters) {
    super(expression, parameters);
    Preconditions.checkArgument(getOutputType().equals(Type.BOOLEAN_TYPE));
  }

  /**
   * Compiles the {@link #javaExpression}.
   *
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    try {
      IExpressionEvaluator se =
          CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

      evaluator =
          (BooleanEvalInterface)
              se.createFastEvaluator(
                  getJavaExpressionWithAppend(),
                  BooleanEvalInterface.class,
                  new String[] {Expression.TB, Expression.ROW});
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}.
   *
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public boolean eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(
        evaluator != null, "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(tb, rowId);
  }
}
