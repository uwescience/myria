package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;

/**
 * An ExpressionOperator with one child.
 */
public abstract class BinaryExpression extends ExpressionOperator {

  /***/
  private static final long serialVersionUID = 1L;

  /** The left child. */
  @JsonProperty
  private final ExpressionOperator left;
  /** The right child. */
  @JsonProperty
  private final ExpressionOperator right;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  protected BinaryExpression() {
    left = null;
    right = null;
  }

  /**
   * @param left the left child.
   * @param right the right child.
   */
  protected BinaryExpression(final ExpressionOperator left, final ExpressionOperator right) {
    this.left = left;
    this.right = right;
  }

  /**
   * @return the left child;
   */
  public final ExpressionOperator getLeft() {
    return left;
  }

  /**
   * @return the right child;
   */
  public final ExpressionOperator getRight() {
    return right;
  }

  /**
   * Returns the infix binary string: "(" + left + infix + right + ")". E.g, for {@link PlusExpression},
   * <code>infix</code> is <code>"+"</code>.
   * 
   * @param infix the string representation of the Operator.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  protected final String getInfixBinaryString(final String infix, final Schema schema) {
    return new StringBuilder("(").append(getLeft().getJavaString(schema)).append(infix).append(
        getRight().getJavaString(schema)).append(')').toString();
  }

  /**
   * Returns the function call binary string: functionName + '(' + left + ',' + right + ")". E.g, for
   * {@link PowExpression}, <code>functionName</code> is <code>"Math.pow"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  protected final String getFunctionCallBinaryString(final String functionName, final Schema schema) {
    return new StringBuilder(functionName).append('(').append(getLeft().getJavaString(schema)).append(',').append(
        getRight().getJavaString(schema)).append(')').toString();
  }

  /**
   * A function that could be used as the default hash code for a binary expression.
   * 
   * @return a hash of (getClass().getCanonicalName(), left, right).
   */
  public final int defaultHashCode() {
    return Objects.hash(getClass().getCanonicalName(), left, right);
  }
}
