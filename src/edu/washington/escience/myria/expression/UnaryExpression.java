package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;

/**
 * An ExpressionOperator with one child.
 */
public abstract class UnaryExpression extends ExpressionOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  protected UnaryExpression() {
    operand = null;
  }

  /**
   * The child expression.
   */
  @JsonProperty
  private final ExpressionOperator operand;

  /**
   * @param operand the operand.
   */
  protected UnaryExpression(final ExpressionOperator operand) {
    this.operand = operand;
  }

  /**
   * @return the child expression.
   */
  public final ExpressionOperator getOperand() {
    return operand;
  }

  /**
   * Returns the function call unary string: functionName + '(' + child + ")". E.g, for {@link SqrtExpression},
   * <code>functionName</code> is <code>"Math.sqrt"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  protected final String getFunctionCallUnaryString(final String functionName, final Schema schema) {
    return new StringBuilder(functionName).append('(').append(operand.getJavaString(schema)).append(')').toString();
  }

  /**
   * Returns the function call unary string: child + functionName. E.g, for {@link ToUpperCaseExpression},
   * <code>functionName</code> is <code>".toUpperCase()"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  protected final String getDotFunctionCallUnaryString(final String functionName, final Schema schema) {
    return new StringBuilder(operand.getJavaString(schema)).append(functionName).toString();
  }

  /**
   * A function that could be used as the default hash code for a unary expression.
   * 
   * @return a hash of (getClass().getCanonicalName(), operand).
   */
  public final int defaultHashCode() {
    return Objects.hash(getClass().getCanonicalName(), operand);
  }
}
