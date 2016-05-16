package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Return the a hash representation as long of the operand.
 */
public class HashMd5Expression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private HashMd5Expression() {
    super();
  }

  /**
   * Compute MD5 hash code.
   *
   * @param operand the operand.
   */
  public HashMd5Expression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    ImmutableList<Type> validTypes =
        ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    int operandIdx = validTypes.indexOf(operandType);
    Preconditions.checkArgument(
        operandIdx != -1,
        "%s cannot handle operand [%s] of Type %s",
        getClass().getSimpleName(),
        getOperand(),
        operandType);
    return Type.LONG_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    Type operandType = getOperand().getOutputType(parameters);
    if (operandType == Type.LONG_TYPE) {
      return getFunctionCallUnaryString("Hashing.md5().hashLong", parameters).concat(".asLong()");
    } else if (operandType == Type.INT_TYPE) {
      return getFunctionCallUnaryString("Hashing.md5().hashInt", parameters).concat(".asLong()");
    } else {
      return new StringBuilder("Hashing.md5().hashString(")
          .append(getOperand().getJavaString(parameters))
          .append(", Charset.defaultCharset()).asLong()")
          .toString();
    }
  }
}
