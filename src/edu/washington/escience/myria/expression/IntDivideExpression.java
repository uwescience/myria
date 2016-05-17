package edu.washington.escience.myria.expression;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Divide two operands in an expression tree. The return value is of type {@link Type.INT_TYPE} if both operands are
 * also INTs, and of type {@link Type.LONG_TYPE} otherwise.
 */
public class IntDivideExpression extends BinaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private IntDivideExpression() {}

  /**
   * Divide the two operands together.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public IntDivideExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  /** The types that this expression might output. */
  private final Set<Type> validTypes =
      ImmutableSet.<Type>builder().add(Type.LONG_TYPE).add(Type.INT_TYPE).build();

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type possibleType = checkAndReturnDefaultNumericType(parameters);
    if (validTypes.contains(possibleType)) {
      return possibleType;
    }
    return Type.LONG_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    Type defaultType = checkAndReturnDefaultNumericType(parameters);
    if (validTypes.contains(defaultType)) {
      /* Default type is an Int or Long, so just use Java's default division. */
      return getInfixBinaryString("/", parameters);
    }
    /* Default type if a Float or Double, so cast in a checked-way to a long. */
    return new StringBuilder("com.google.common.math.DoubleMath.roundToLong(")
        .append(getInfixBinaryString("/", parameters))
        .append(", java.math.RoundingMode.DOWN)")
        .toString();
  }
}
