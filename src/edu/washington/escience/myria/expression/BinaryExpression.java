package edu.washington.escience.myria.expression;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * An ExpressionOperator with two children.
 */
public abstract class BinaryExpression extends ExpressionOperator {

  /***/
  private static final long serialVersionUID = 1L;

  /** The left child. */
  @JsonProperty private final ExpressionOperator left;
  /** The right child. */
  @JsonProperty private final ExpressionOperator right;

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

  @Override
  public List<ExpressionOperator> getChildren() {
    ImmutableList.Builder<ExpressionOperator> children = ImmutableList.builder();
    return children.add(getLeft()).add(getRight()).build();
  }

  /**
   * Returns the infix binary string: "(" + left + infix + right + ")". E.g, for {@link PlusExpression},
   * <code>infix</code> is <code>"+"</code>.
   *
   * @param infix the string representation of the Operator.
   * @param parameters parameters that are needed to create the java expression
   * @return the Java string for this operator.
   */
  protected final String getInfixBinaryString(
      final String infix, final ExpressionOperatorParameter parameters) {
    return new StringBuilder("(")
        .append(getLeft().getJavaString(parameters))
        .append(infix)
        .append(getRight().getJavaString(parameters))
        .append(')')
        .toString();
  }

  /**
   * Returns the object comparison string: left + ".compareTo(" + right + ")" + op + "0". E.g, for
   * {@link EqualsExpression}, <code>op</code> is <code>==</code> and <code>value</code> is <code>0</code>.
   *
   * @param op integer comparison operator >, <, ==, >=, <=.
   * @param parameters parameters that are needed to create the java expression
   * @return the Java string for this operator.
   */
  protected final String getObjectComparisonString(
      final SimplePredicate.Op op, final ExpressionOperatorParameter parameters) {
    return new StringBuilder("(")
        .append(getLeft().getJavaString(parameters))
        .append(".compareTo(")
        .append(getRight().getJavaString(parameters))
        .append(')')
        .append(op.toJavaString())
        .append(0)
        .append(")")
        .toString();
  }

  /**
   * Returns the function call binary string: functionName + '(' + left + ',' + right + ")". E.g, for
   * {@link PowExpression}, <code>functionName</code> is <code>"Math.pow"</code>.
   *
   * @param functionName the string representation of the Java function name.
   * @param parameters parameters that are needed to create the java expression
   * @return the Java string for this operator.
   */
  protected final String getFunctionCallBinaryString(
      final String functionName, final ExpressionOperatorParameter parameters) {
    return new StringBuilder(functionName)
        .append('(')
        .append(getLeft().getJavaString(parameters))
        .append(',')
        .append(getRight().getJavaString(parameters))
        .append(')')
        .toString();
  }

  /**
   * Returns the left function call string: functionName + '(' + left+ ')'.
   *
   * @param functionName the string representation of the Java function name.
   * @param parameters parameters that are needed to create the java expression
   * @return the Java string for this operator.
   */
  protected final String getLeftFunctionCallString(
      final String functionName, final ExpressionOperatorParameter parameters) {
    return new StringBuilder(functionName)
        .append('(')
        .append(getLeft().getJavaString(parameters))
        .append(')')
        .toString();
  }

  /**
   * Returns the left function call string with additional parameter: functionName+'('+left+','+'parameter'+')'.
   *
   * @param functionName the string representation of the Java function name.
   * @param parameters parameters that are needed to create the java expression
   * @param additionalParameter user specified additional parameter.
   * @return the Java string for this operator.
   */
  protected final String getLeftFunctionCallWithParameterString(
      final String functionName,
      final ExpressionOperatorParameter parameters,
      final String additionalParameter) {
    return new StringBuilder(functionName)
        .append('(')
        .append(getLeft().getJavaString(parameters))
        .append(",")
        .append(additionalParameter)
        .append(')')
        .toString();
  }

  /**
   * A function that could be used as the default hash code for a binary expression.
   *
   * @return a hash of (getClass().getCanonicalName(), left, right).
   */
  protected final int defaultHashCode() {
    return Objects.hash(getClass().getCanonicalName(), left, right);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    BinaryExpression otherExpr = (BinaryExpression) other;
    return Objects.equals(left, otherExpr.left) && Objects.equals(right, otherExpr.right);
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * numeric.
   *
   * @param parameters parameters that are needed to determine the output type
   * @return the default numeric type, based on the types of the children and Java type precedence.
   */
  protected final Type checkAndReturnDefaultNumericType(
      final ExpressionOperatorParameter parameters) {
    return checkAndReturnDefaultNumericType(
        parameters,
        ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE));
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * numeric.
   *
   * @param parameters parameters that are needed to determine the output type
   * @param validTypes a list of valid types ordered by their precedence
   * @return the default numeric type, based on the types of the children and type precedence.
   */
  protected final Type checkAndReturnDefaultNumericType(
      final ExpressionOperatorParameter parameters, final List<Type> validTypes) {
    Type leftType = getLeft().getOutputType(parameters);
    Type rightType = getRight().getOutputType(parameters);
    int leftIdx = validTypes.indexOf(leftType);
    int rightIdx = validTypes.indexOf(rightType);
    Preconditions.checkArgument(
        leftIdx != -1,
        "%s cannot handle left child [%s] of Type %s",
        getClass().getSimpleName(),
        getLeft(),
        leftType);
    Preconditions.checkArgument(
        rightIdx != -1,
        "%s cannot handle right child [%s] of Type %s",
        getClass().getSimpleName(),
        getRight(),
        rightType);
    return validTypes.get(Math.min(leftIdx, rightIdx));
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * boolean.
   *
   * @param parameters parameters that are needed to determine the output type
   */
  protected final void checkBooleanType(final ExpressionOperatorParameter parameters) {
    Type leftType = getLeft().getOutputType(parameters);
    Type rightType = getRight().getOutputType(parameters);
    Preconditions.checkArgument(
        leftType == Type.BOOLEAN_TYPE,
        "%s cannot handle left child [%s] of Type %s",
        getClass().getSimpleName(),
        getLeft(),
        leftType);
    Preconditions.checkArgument(
        rightType == Type.BOOLEAN_TYPE,
        "%s cannot handle right child [%s] of Type %s",
        getClass().getSimpleName(),
        getRight(),
        rightType);
  }
}
