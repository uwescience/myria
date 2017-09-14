package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

public class ConcatExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ConcatExpression() {}

  public ConcatExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(ExpressionOperatorParameter parameters) {
    Preconditions.checkArgument(
        getChildren().size() == 2, "Concat function has to take 2 arguments.");
    Preconditions.checkArgument(
        getChildren().get(0).getOutputType(parameters) == Type.STRING_TYPE,
        "The first argument of concat has to be STRING.");
    Preconditions.checkArgument(
        getChildren().get(1).getOutputType(parameters) == Type.STRING_TYPE,
        "The second argument of concat has to be STRING.");
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(ExpressionOperatorParameter parameters) {
    return getInfixBinaryString("+", parameters);
  }
}
