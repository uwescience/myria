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
    LONGTOINT,
    /**
     * from float or double to int.
     */
    FLOATSTOINT,
    /**
     * from number to float.
     */
    NUMTOFLOAT,
    /**
     * from number to double.
     */
    NUMTODOUBLE,
    /**
     * from int to long.
     */
    INTTOLONG,
    /**
     * from float or double to long.
     */
    FLOATSTOLONG,
    /**
     * from anything to string.
     */
    TOSTR,
    /**
     * from string to int.
     */
    STRTOINT,
    /**
     * from string to float.
     */
    STRTOFLOAT,
    /**
     * from string to double.
     */
    STRTODOUBLE,
    /**
     * from string to long.
     */
    STRTOLONG,
    /**
     * unsupported cast.
     */
    UNSUPPORTED,
  }

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CastExpression() {
  }

  /**
   * @param left what to cast.
   * @param right the output type of this operand is used to determine what to cast to.
   */
  public CastExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
    Preconditions.checkArgument(right instanceof TypeExpression || right instanceof TypeOfExpression,
        "The right child of a cast operator must be a Type or a TypeOf, not a %s", right.getClass().getSimpleName());
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    final Type castFrom = getLeft().getOutputType(parameters);
    final Type castTo = getRight().getOutputType(parameters);
    Preconditions.checkArgument(getCastType(castFrom, castTo) != CastType.UNSUPPORTED,
        "Cast from %s to %s is not supported.", castFrom, castTo);
    return castTo;
  }

  /**
   * @param castTo the type that we cast to
   * @param castFrom the type that we cast from
   * @return true if the cast can be done using value of
   */
  private CastType getCastType(final Type castFrom, final Type castTo) {

    String cast = castFrom + " " + castTo;
    switch (cast) {
      case "INT_TYPE STRING_TYPE":
      case "FLOAT_TYPE STRING_TYPE":
      case "DOUBLE_TYPE STRING_TYPE":
      case "LONG_TYPE STRING_TYPE":
      case "DATETIME_TYPE STRING_TYPE":
        return CastType.TOSTR;
      case "LONG_TYPE INT_TYPE":
        return CastType.LONGTOINT;
      case "FLOAT_TYPE INT_TYPE":
      case "DOUBLE_TYPE INT_TYPE":
        return CastType.FLOATSTOINT;
      case "INT_TYPE FLOAT_TYPE":
      case "LONG_TYPE FLOAT_TYPE":
      case "DOUBLE_TYPE FLOAT_TYPE":
        return CastType.NUMTOFLOAT;
      case "INT_TYPE DOUBLE_TYPE":
      case "LONG_TYPE DOUBLE_TYPE":
      case "FLOAT_TYPE DOUBLE_TYPE":
        return CastType.NUMTODOUBLE;
      case "INT_TYPE LONG_TYPE":
        return CastType.INTTOLONG;
      case "FLOAT_TYPE LONG_TYPE":
      case "DOUBLE_TYPE LONG_TYPE":
        return CastType.FLOATSTOLONG;
      case "STRING_TYPE INT_TYPE":
        return CastType.STRTOINT;
      case "STRING_TYPE FLOAT_TYPE":
        return CastType.STRTOFLOAT;
      case "STRING_TYPE DOUBLE_TYPE":
        return CastType.STRTODOUBLE;
      case "STRING_TYPE LONG_TYPE":
        return CastType.STRTOLONG;
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
  private String getPrimitiveTypeCastString(final String targetType, final ExpressionOperatorParameter parameters) {
    return new StringBuilder().append("((").append(targetType).append(")(").append(getLeft().getJavaString(parameters))
        .append("))").toString();
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    final Type castFrom = getLeft().getOutputType(parameters);
    final Type castTo = getRight().getOutputType(parameters);
    // use primitive type conversion for efficiency.
    switch (getCastType(castFrom, castTo)) {
      case LONGTOINT:
        return getLeftFunctionCallString("com.google.common.primitives.Ints.checkedCast", parameters);
      case FLOATSTOINT:
        return getLeftFunctionCallWithParemeterString("com.google.common.math.DoubleMath.roundToInt", parameters,
            "java.math.RoundingMode.DOWN");
      case NUMTOFLOAT:
        return getPrimitiveTypeCastString("float", parameters);
      case NUMTODOUBLE:
        return getPrimitiveTypeCastString("double", parameters);
      case INTTOLONG:
        return getPrimitiveTypeCastString("long", parameters);
      case FLOATSTOLONG:
        return getLeftFunctionCallWithParemeterString("com.google.common.math.DoubleMath.roundToLong", parameters,
            "java.math.RoundingMode.DOWN");
      case TOSTR:
        return getLeftFunctionCallString("String.valueOf", parameters);
      case STRTOINT:
        return getLeftFunctionCallString("Integer.parseInt", parameters);
      case STRTOFLOAT:
        return getLeftFunctionCallString("Float.parseFloat", parameters);
      case STRTODOUBLE:
        return getLeftFunctionCallString("Double.parseDouble", parameters);
      case STRTOLONG:
        return getLeftFunctionCallString("Long.parseLong", parameters);
      default:
        throw new IllegalStateException("should not reach here.");
    }
  }
}
