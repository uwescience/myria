package edu.washington.escience.myria.expression;

import java.io.IOException;
import java.net.URISyntaxException;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

public class DownloadBlobExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private DownloadBlobExpression() {
    super();
  }

  /**
   * @param operand the operand.
   */
  public DownloadBlobExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return Type.BLOB_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder("edu.washington.escience.myria.util.MyriaUtils.getBlob(")
        .append(getOperand().getJavaString(parameters))
        .append(")")
        .toString();
  }
}
