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
    }
    return CastType.UNSUPPORTED;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    final Type castFrom = getLeft().getOutputType(parameters);
    final Type castTo = getRight().getOutputType(parameters);
    // use primitive type conversion for efficiency.
    switch (getCastType(castFrom, castTo)) {
      case NUMTOINT:
        return new StringBuilder().append("((int)(").append(getLeft().getJavaString(parameters)).append("))")
            .toString();
      case NUMTOFLOAT:
        return new StringBuilder().append("((float)(").append(getLeft().getJavaString(parameters)).append("))")
            .toString();
      case NUMTODOUBLE:
        return new StringBuilder().append("((double)(").append(getLeft().getJavaString(parameters)).append("))")
            .toString();
      case NUMTOLONG:
        return new StringBuilder().append("((long)(").append(getLeft().getJavaString(parameters)).append("))")
            .toString();
      case TOSTR:
        return new StringBuilder().append(getLeft().getJavaString(parameters)).append(".toString()").toString();
      case STRTOINT:
        return new StringBuilder().append("Integer.parseInt(").append(getLeft().getJavaString(parameters)).append(")")
            .toString();
      case STRTOFLOAT:
        return new StringBuilder().append("Float.parseFloat(").append(getLeft().getJavaString(parameters)).append(")")
            .toString();
      case STRTODOUBLE:
        return new StringBuilder().append("Double.parseDouble(").append(getLeft().getJavaString(parameters))
            .append(")").toString();
      case STRTOLONG:
        return new StringBuilder().append("Long.parseLong(").append(getLeft().getJavaString(parameters)).append(")")
            .toString();
      default:
        throw new IllegalStateException("should not reach here.");
    }

  }
}
