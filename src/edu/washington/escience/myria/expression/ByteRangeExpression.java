package edu.washington.escience.myria.expression;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Get a byte range from a blob.
 */
public class ByteRangeExpression extends NAryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ByteRangeExpression() {
    super();
  }

  /**
   * Returns a byte range from a blob.
   *
   * @param blob the blob.
   * @param beginIdx begin index of the byte range, inclusive.
   * @param endIdx end index of the byte range, exclusive.
   */
  public ByteRangeExpression(
      final ExpressionOperator blob,
      final ExpressionOperator beginIdx,
      final ExpressionOperator endIdx) {
    super(ImmutableList.of(blob, beginIdx, endIdx));
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Preconditions.checkArgument(
        getChildren().size() == 3, "BYTERANGE function has to take 3 arguments.");
    Preconditions.checkArgument(
        getChildren().get(0).getOutputType(parameters) == Type.BLOB_TYPE,
        "The first argument of BYTERANGE has to be BLOB.");
    Preconditions.checkArgument(
        getChildren().get(1).getOutputType(parameters) == Type.INT_TYPE,
        "The second argument of BYTERANGE has to be INT.");
    Preconditions.checkArgument(
        getChildren().get(2).getOutputType(parameters) == Type.INT_TYPE,
        "The third argument of BYTERANGE has to be INT.");
    return Type.BLOB_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("edu.washington.escience.myria.util.MyriaUtils.byteRange")
        .append("(")
        .append(getChildren().get(0).getJavaString(parameters))
        .append(",")
        .append(getChildren().get(1).getJavaString(parameters))
        .append(",")
        .append(getChildren().get(2).getJavaString(parameters))
        .append(")")
        .toString();
  }
}
