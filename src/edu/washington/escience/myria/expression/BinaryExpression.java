package edu.washington.escience.myria.expression;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;

/**
 * An ExpressionOperator with one child.
 */
public abstract class BinaryExpression extends ExpressionOperator {
  /** The left child. */
  @JsonProperty("left")
  private final ExpressionOperator left;
  /** The right child. */
  @JsonProperty("right")
  private final ExpressionOperator right;

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

  @Override
  @JsonIgnore
  public final Set<VariableExpression> getVariables() {
    ImmutableSet.Builder<VariableExpression> builder = ImmutableSet.builder();
    builder.addAll(left.getVariables());
    builder.addAll(right.getVariables());
    return builder.build();
  }

  /**
   * Returns the infix binary string: "(" + left + infix + right + ")". E.g, for {@link PlusExpression},
   * <code>infix</code> is <code>"+"</code>.
   * 
   * @param infix the string representation of the Operator.
   * @param schema the input schema
   * @return the Java string for this operator.
   */
  @JsonIgnore
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
  @JsonIgnore
  protected final String getFunctionCallBinaryString(final String functionName, final Schema schema) {
    return new StringBuilder(functionName).append('(').append(getLeft().getJavaString(schema)).append(',').append(
        getRight().getJavaString(schema)).append(')').toString();
  }
}
