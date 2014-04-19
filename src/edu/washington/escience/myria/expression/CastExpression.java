package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
     * from number to int.
     */
    NUMTOINT,
    /**
     * from number to float.
     */
    NUMTOFLOAT,
    /**
     * from number to double.
     */
    NUMTODOUBLE,
    /**
     * from number to long.
     */
    NUMTOLONG,
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
    if (getCastType(castFrom, castTo) != CastType.UNSUPPORTED) {
      return castTo;
    } else {
      throw new IllegalStateException(String.format("Cast from %s to %s is not supported.", castFrom, castTo));
    }
  }

  /**
   * @param castTo the type that we cast to
   * @param castFrom the type that we cast from
   * @return type of cast
   */
  private CastType getCastType(final Type castFrom, final Type castTo) {
    ImmutableList<Type> numericTypes =
        ImmutableList.of(Type.INT_TYPE, Type.FLOAT_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE);
    if (castTo == Type.STRING_TYPE) {
      return CastType.TOSTR;
    } else if (numericTypes.contains(castFrom)) {
      switch (castTo) {
        case INT_TYPE:
          return CastType.NUMTOINT;
        case FLOAT_TYPE:
          return CastType.NUMTOFLOAT;
        case DOUBLE_TYPE:
          return CastType.NUMTODOUBLE;
        case LONG_TYPE:
          return CastType.NUMTOLONG;
        default:
          break;
      }
    } else if (castFrom == Type.STRING_TYPE) {
      switch (castTo) {
        case INT_TYPE:
          return CastType.STRTOINT;
        case FLOAT_TYPE:
          return CastType.STRTOFLOAT;
        case DOUBLE_TYPE:
          return CastType.STRTODOUBLE;
        case LONG_TYPE:
          return CastType.STRTOLONG;
        default:
          break;
      }
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
      case NUMTOINT:
        return getPrimitiveTypeCastString("int", parameters);
      case NUMTOFLOAT:
        return getPrimitiveTypeCastString("float", parameters);
      case NUMTODOUBLE:
        return getPrimitiveTypeCastString("double", parameters);
      case NUMTOLONG:
        return getPrimitiveTypeCastString("long", parameters);
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
