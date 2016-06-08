package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Get a substring from a string.
 */
public class SubstrExpression extends NAryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private SubstrExpression() {
    super();
  }

  /**
   * Get a substring of a string.
   *
   * @param str the string.
   * @param beginIdx begin index of the substring, inclusive.
   * @param endIdx end index of the substring, exclusive.
   */
  public SubstrExpression(
      final ExpressionOperator str,
      final ExpressionOperator beginIdx,
      final ExpressionOperator endIdx) {
    super(ImmutableList.of(str, beginIdx, endIdx));
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Preconditions.checkArgument(
        getChildren().size() == 3, "Substr function has to take 3 arguments.");
    Preconditions.checkArgument(
        getChildren().get(0).getOutputType(parameters) == Type.STRING_TYPE,
        "The first argument of substr has to be STRING.");
    Preconditions.checkArgument(
        getChildren().get(1).getOutputType(parameters) == Type.INT_TYPE,
        "The second argument of substr has to be INT.");
    Preconditions.checkArgument(
        getChildren().get(2).getOutputType(parameters) == Type.INT_TYPE,
        "The third argument of substr has to be INT.");
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return getDotFunctionCallString(".substring", parameters);
  }
}
