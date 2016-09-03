package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Returns a sequence of substrings of a given string, split on a regular expression.
 */
public class SplitExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private SplitExpression() {
    super();
  }

  /**
   * Takes the string to be split and the regex to split it on.
   *
   * @param left the left operand (index of string column).
   * @param right the right operand (regular expression string).
   */
  public SplitExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkOperandTypes(Type.STRING_TYPE, Type.STRING_TYPE, parameters);
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("java.util.regex.Pattern.compile(")
        .append(getRight().getJavaString(parameters))
        .append(")")
        .append(".split(")
        .append(getLeft().getJavaString(parameters))
        .append(", -1)")
        .toString();
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }
}
