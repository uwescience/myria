package edu.washington.escience.myria.expression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Cast the output from an expression to another type.
 */
public class CastExpression extends UnaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /** The type of this object. */
  @JsonProperty
  private final Type valueType;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CastExpression() {
    valueType = null;
  }

  /**
   * @param operand the operand
   * @param type the type of this object.
   */
  public CastExpression(final ExpressionOperator operand, final Type type) {
    super(operand);
    valueType = type;
  }

  @Override
  public Type getOutputType(final Schema schema) {
    // TODO support more than just casting from object, i.e. from string to int
    Preconditions.checkArgument(getOperand().getOutputType(schema) == Type.OBJ_TYPE);
    return valueType;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return new StringBuilder().append("((").append(valueType.toJavaObjectType().getSimpleName()).append(")").append(
        getOperand().getJavaString(schema)).append(")").toString();
  }
}
