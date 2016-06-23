/**
 *
 */
package edu.washington.escience.myria.expression;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * 
 */
public class PyUDFExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PyUDFExpression.class);

  /** The name of the python function. */
  @JsonProperty
  private final String name;

  // private int leftColumnIdx;
  // private int rightColumnIdx;
  // private boolean bLeftState;
  // private boolean bRightState;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private PyUDFExpression() {
    name = "";
  }

  public PyUDFExpression(final ExpressionOperator left, final ExpressionOperator right, final String name) {
    super(left, right);
    this.name = name;

    LOGGER.info("left string :" + left.toString());
    LOGGER.info("right string :" + right.toString());
    // setColumnId(left, right);

  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    // look at the output schema of from the expressionOperatorParameter?
    return Type.BYTES_TYPE;

  }

  /**
   * @return the column index of this variable.
   */
  public String getName() {
    return name;
  }

  // don't need the getJavaSubstring
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return "";
  }

}
