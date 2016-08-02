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

  /** The name of the python function. */
  @JsonProperty private final String name;

  /**
   * Output Type of python function.
   */
  @JsonProperty private final Type outputType;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private PyUDFExpression() {
    name = "";
    outputType = Type.BYTES_TYPE;
  }

  /**
   * Python function expression
   *
   * @param left the left operand.
   * @param right the right operand.
   * @param Type - output Type of the python function
   * @param name - name of the python function in the catalog
   */
  public PyUDFExpression(
      final ExpressionOperator left,
      final ExpressionOperator right,
      final String name,
      final Type outputType) {
    super(left, right);
    this.name = name;
    this.outputType = outputType;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }

  /**
   * @return the column index of this variable.
   */
  public String getName() {
    return name;
  }

  /**
   * @return the output Type for this python expression.
   */
  public Type getOutput() {
    return outputType;
  }

  // don't need the getJavaSubstring
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return "";
  }
}
