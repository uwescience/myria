package edu.washington.escience.myria.expression;

import com.fasterxml.jackson.annotation.JsonIgnore;

import edu.washington.escience.myria.Schema;

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

  /**
   * Returns the function call unary string: functionName + '(' + child + ")". E.g, for {@link SqrtExpression},
   * <code>functionName</code> is <code>"Math.sqrt"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  @JsonIgnore
  protected final String getFunctionCallUnaryString(final String functionName, final Schema schema) {
    return new StringBuilder(functionName).append('(').append(child.getJavaString(schema)).append(')').toString();
  }

  /**
   * Returns the function call unary string: child + functionName. E.g, for {@link ToUpperCaseExpression},
   * <code>functionName</code> is <code>".toUpperCase()"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  @JsonIgnore
  protected final String getDotFunctionCallUnaryString(final String functionName, final Schema schema) {
    return new StringBuilder(child.getJavaString(schema)).append(functionName).toString();
  }
}
