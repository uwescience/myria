package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return a sequence of numbers from 0 (inclusive) to operand (exclusive).
 */
public class SequenceExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private SequenceExpression() {
    super();
  }

  /**
   * Takes the upper bound of the sequence to be returned.
   *
   * @param operand an expression that evaluates to a positive integer
   */
  public SequenceExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkOperandType(Type.INT_TYPE, parameters);
    return Type.INT_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("IntStream.range(0, (")
        .append(getOperand().getJavaString(parameters))
        .append(")).toArray()")
        .toString();
  }

  @Override
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append(Expression.COUNT)
        .append(".appendInt((int) (")
        .append(getOperand().getJavaString(parameters))
        .append("));\n")
        // It would be nice to replace this loop with IntStream.forEach(), but Janino doesn't support lambdas.
        .append("for (int i = 0; i < (int) (")
        .append(getOperand().getJavaString(parameters))
        .append("); ++i) {\n")
        .append(Expression.RESULT)
        .append(".appendInt(i);\n}")
        .toString();
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }
}
