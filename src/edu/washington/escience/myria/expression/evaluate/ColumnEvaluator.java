package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;

/**
 * 
 */
public class ColumnEvaluator extends Evaluator {
  /**
   * @param expression the expression to be evaluated
   * @param inputSchema the schema that the expression expects if it operates on a schema
   * @param stateSchema the schema of the state
   */
  public ColumnEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
  }

  /**
   * @return the Java form of this expression.
   */
  public String getJavaExpression() {
    return getExpression().getJavaExpression(getInputSchema(), getStateSchema());
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return getExpression().getOutputName();
  }

  /**
   * @return true if the expression just copies a column from the input
   */
  public boolean isCopyFromInput() {
    final ExpressionOperator rootOp = getExpression().getRootExpressionOperator();
    return rootOp instanceof VariableExpression;
  }

  /**
   * An expression does not have to be compiled when it only renames or copies a column. This is an optimization to
   * avoid evaluating the expression and avoid autoboxing values.
   * 
   * @return true if the expression does not have to be compiled.
   */
  public boolean needsCompiling() {
    return !(isCopyFromInput() || isConstant());
  }

  /**
   * @return true if the expression is a constant expression
   */
  public boolean isConstant() {
    final ExpressionOperator rootOp = getExpression().getRootExpressionOperator();
    return rootOp instanceof ConstantExpression;
  }
}
