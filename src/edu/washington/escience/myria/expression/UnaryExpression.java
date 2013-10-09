package edu.washington.escience.myria.expression;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * An ExpressionOperator with one child.
 */
public abstract class UnaryExpression extends ExpressionOperator {
  /**
   * The child expression.
   */
  private final ExpressionOperator child;

  /**
   * @param child the child expression.
   */
  protected UnaryExpression(final ExpressionOperator child) {
    this.child = child;
  }

  /**
   * @return the child expression.
   */
  public final ExpressionOperator getChild() {
    return child;
  }

  @Override
  public Set<VariableExpression> getVariables() {
    return child.getVariables();
  }

  /**
   * Returns the function call binary string: functionName + '(' + left + ',' + right + ")". E.g, for
   * {@link PowExpression}, <code>functionName</code> is <code>"Math.pow"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @return the Java string for this operator.
   */
  @JsonIgnore
  protected final String getFunctionCallUnaryString(final String functionName) {
    return new StringBuilder(functionName).append('(').append(child.getJavaString()).append(')').toString();
  }
}
