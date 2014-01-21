package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Cast the output from an expression to another type.
 */
public class CastExpression extends BinaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

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
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    // TODO support more than just casting from object, i.e. from string to int
    final Type castFrom = getLeft().getOutputType(schema, stateSchema);
    final Type castTo = getRight().getOutputType(schema, stateSchema);
    if (isSimpleCast(castFrom, castTo)) {
      return castTo;
    } else {
      throw new IllegalStateException(String.format("Cannot cast from %s to %s", castFrom, castTo));
    }
  }

  /**
   * @param castTo the type that we cast to
   * @param castFrom the type that we cast from
   * @return true if the cast can be done using value of
   */
  private boolean isSimpleCast(final Type castFrom, final Type castTo) {
    return (castFrom == Type.INT_TYPE || castFrom == Type.FLOAT_TYPE || castFrom == Type.DOUBLE_TYPE || castFrom == Type.LONG_TYPE)
        && (castTo == Type.INT_TYPE || castTo == Type.FLOAT_TYPE || castTo == Type.DOUBLE_TYPE || castTo == Type.LONG_TYPE);
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    // final Type castFrom = getLeft().getOutputType(schema, stateSchema);
    final Type castTo = getRight().getOutputType(schema, stateSchema);
    return new StringBuilder().append("(").append(castTo.toJavaObjectType().getSimpleName()).append(".valueOf(")
        .append(getLeft().getJavaString(schema, stateSchema)).append("))").toString();
  }
}
