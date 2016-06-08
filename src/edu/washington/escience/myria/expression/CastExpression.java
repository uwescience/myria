package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Cast the output from an expression to another type.
 */
public class CastExpression extends BinaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Type of casting.
   */
  private enum CastType {
    /**
     * from long to int.
     */
    LONG_TO_INT,
    /**
     * from float or double to int.
     */
    FLOATS_TO_INT,
    /**
     * int or long to float.
     */
    INT_OR_LONG_TO_FLOAT,
    /**
     * from double to float.
     */
    DOUBLE_TO_FLOAT,
    /**
     * from number to double.
     */
    NUM_TO_DOUBLE,
    /**
     * from int to long.
     */
    INT_TO_LONG,
    /**
     * from float or double to long.
     */
    FLOATS_TO_LONG,
    /**
     * from anything to string.
     */
    TO_STR,
    /**
     * from string to int.
     */
    STR_TO_INT,
    /**
     * from string to float.
     */
    STR_TO_FLOAT,
    /**
     * from string to double.
     */
    STR_TO_DOUBLE,
    /**
     * from string to long.
     */
    STR_TO_LONG,
    /**
     * unsupported cast.
     */
    UNSUPPORTED,
  }

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CastExpression() {}

  /**
   * @param left what to cast.
   * @param right the output type of this operand is used to determine what to cast to.
   */
  public CastExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
    Preconditions.checkArgument(
        right instanceof TypeExpression || right instanceof TypeOfExpression,
        "The right child of a cast operator must be a Type or a TypeOf, not a %s",
        right.getClass().getSimpleName());
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    final Type castFrom = getLeft().getOutputType(parameters);
    final Type castTo = getRight().getOutputType(parameters);
    Preconditions.checkArgument(
        (castFrom == castTo) || getCastType(castFrom, castTo) != CastType.UNSUPPORTED,
        "Cast from %s to %s is not supported.",
        castFrom,
        castTo);
    return castTo;
  }

  /**
   * @param castTo the type that we cast to
   * @param castFrom the type that we cast from
   * @return true if the cast can be done using value of
   */
  private CastType getCastType(final Type castFrom, final Type castTo) {

    switch (castFrom + "|" + castTo) {
        /* any type to string. */
      case "INT_TYPE|STRING_TYPE":
      case "FLOAT_TYPE|STRING_TYPE":
      case "DOUBLE_TYPE|STRING_TYPE":
      case "LONG_TYPE|STRING_TYPE":
      case "DATETIME_TYPE|STRING_TYPE":
        return CastType.TO_STR;
        /* numeric type to int. */
      case "LONG_TYPE|INT_TYPE":
        return CastType.LONG_TO_INT;
      case "FLOAT_TYPE|INT_TYPE":
      case "DOUBLE_TYPE|INT_TYPE":
        return CastType.FLOATS_TO_INT;
        /* numeric type to float. */
      case "INT_TYPE|FLOAT_TYPE":
      case "LONG_TYPE|FLOAT_TYPE":
        return CastType.INT_OR_LONG_TO_FLOAT;
      case "DOUBLE_TYPE|FLOAT_TYPE":
        return CastType.DOUBLE_TO_FLOAT;
        /* numeric type to double. */
      case "INT_TYPE|DOUBLE_TYPE":
      case "LONG_TYPE|DOUBLE_TYPE":
      case "FLOAT_TYPE|DOUBLE_TYPE":
        return CastType.NUM_TO_DOUBLE;
        /* numeric type to long. */
      case "INT_TYPE|LONG_TYPE":
        return CastType.INT_TO_LONG;
      case "FLOAT_TYPE|LONG_TYPE":
      case "DOUBLE_TYPE|LONG_TYPE":
        return CastType.FLOATS_TO_LONG;
        /* String to numeric */
      case "STRING_TYPE|INT_TYPE":
        return CastType.STR_TO_INT;
      case "STRING_TYPE|FLOAT_TYPE":
        return CastType.STR_TO_FLOAT;
      case "STRING_TYPE|DOUBLE_TYPE":
        return CastType.STR_TO_DOUBLE;
      case "STRING_TYPE|LONG_TYPE":
        return CastType.STR_TO_LONG;
      default:
        break;
    }
    return CastType.UNSUPPORTED;
  }

  /**
   * Returns the string for a primitive cast: '((' + targetType + ')' + left + ')'.
   *
   * @param targetType string of the type to be casted to.
   * @param parameters parameters that are needed to determine the output type.
   * @return string that used for numeric type cast.
   */
  private String getPrimitiveTypeCastString(
      final String targetType, final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("((")
        .append(targetType)
        .append(")(")
        .append(getLeft().getJavaString(parameters))
        .append("))")
        .toString();
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    final Type castFrom = getLeft().getOutputType(parameters);
    final Type castTo = getRight().getOutputType(parameters);

    /* Trivial casts are, of course, allowed. See also #626. */
    if (castFrom == castTo) {
      return getLeft().getJavaString(parameters);
    }

    switch (getCastType(castFrom, castTo)) {
      case LONG_TO_INT:
        return getLeftFunctionCallString(
            "com.google.common.primitives.Ints.checkedCast", parameters);
      case FLOATS_TO_INT:
        return getLeftFunctionCallWithParameterString(
            "com.google.common.math.DoubleMath.roundToInt",
            parameters,
            "java.math.RoundingMode.DOWN");
      case INT_OR_LONG_TO_FLOAT:
        return getPrimitiveTypeCastString("float", parameters);
      case DOUBLE_TO_FLOAT:
        return getLeftFunctionCallString(
            "edu.washington.escience.myria.util.MathUtils.castDoubleToFloat", parameters);
      case NUM_TO_DOUBLE:
        return getPrimitiveTypeCastString("double", parameters);
      case INT_TO_LONG:
        return getPrimitiveTypeCastString("long", parameters);
      case FLOATS_TO_LONG:
        return getLeftFunctionCallWithParameterString(
            "com.google.common.math.DoubleMath.roundToLong",
            parameters,
            "java.math.RoundingMode.DOWN");
      case TO_STR:
        return getLeftFunctionCallString("String.valueOf", parameters);
      case STR_TO_INT:
        return getLeftFunctionCallString("Integer.parseInt", parameters);
      case STR_TO_FLOAT:
        return getLeftFunctionCallString("Float.parseFloat", parameters);
      case STR_TO_DOUBLE:
        return getLeftFunctionCallString("Double.parseDouble", parameters);
      case STR_TO_LONG:
        return getLeftFunctionCallString("Long.parseLong", parameters);
      default:
        throw new IllegalStateException("should not reach here.");
    }
  }
}
