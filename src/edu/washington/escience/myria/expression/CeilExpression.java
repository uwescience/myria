package edu.washington.escience.myria.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the {@link Math.ceil} of the operand.
 */
public class CeilExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private CeilExpression() {
    super();
  }

  /**
   * Take the {@link Math.ceil} of the operand.
   * 
   * @param operand the operand.
   */
  public CeilExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    Type operandType = getOperand().getOutputType(schema);
    ImmutableList<Type> validTypes = ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(operandIdx != -1, "CeilExpression cannot handle operand [%s] of Type %s", getOperand(),
        operandType);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return getFunctionCallUnaryString("Math.ceil", schema);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof CeilExpression)) {
      return false;
    }
    CeilExpression otherExpr = (CeilExpression) other;
    return Objects.equal(getOperand(), otherExpr.getOperand());
  }
}