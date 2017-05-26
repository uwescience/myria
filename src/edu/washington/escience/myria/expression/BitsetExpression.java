package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Returns all bits in an input of BLOB_TYPE as a sequence of BOOLEAN_TYPE
 * (enumerating the input bytes in little-endian order).
 */
public class BitsetExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private BitsetExpression() {
    super();
  }

  /**
   * Takes an input of BLOB_TYPE.
   *
   * @param operand the input blob
   */
  public BitsetExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkOperandType(Type.BLOB_TYPE, parameters);
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("ByteBuffer bb = (")
        .append(getOperand().getJavaString(parameters))
        .append(");\n")
        .append("java.util.BitSet bs = java.util.BitSet.valueOf(bb);\n")
        .append("int bits_len = bb.capacity() * 8;\n")
        .append("boolean[] vals = new boolean[bits_len];\n")
        .append("for (int i = 0; i < bits_len; ++i) {\n")
        .append("vals[i] = bs.get(i);\n")
        .append("}\n")
        .append("return vals;\n")
        .toString();
  }

  @Override
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("ByteBuffer bb = (")
        .append(getOperand().getJavaString(parameters))
        .append(");\n")
        .append("java.util.BitSet bs = java.util.BitSet.valueOf(bb);\n")
        .append("int bits_len = bb.capacity() * 8;\n")
        .append(Expression.COUNT)
        .append(".appendInt(bits_len);\n")
        .append("for (int i = 0; i < bits_len; ++i) {\n")
        .append(Expression.RESULT)
        .append(".appendBoolean(bs.get(i));\n")
        .append("}\n")
        .toString();
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }
}
