package edu.washington.escience.myria.expression;

import java.util.List;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 *
 * Python expressions.
 *
 */
public class PyUDFExpression extends NAryExpression {
  /***/
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PyUDFExpression.class);

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
    //set default return type this is changed once pythonfunction is retrived.
    outputType = Type.BLOB_TYPE;
  }
  /**
   * Python function expression.
   *
   * @param children operand.
   * @param outputType - output Type of the python function
   * @param name - name of the python function in the catalog
   */
  public PyUDFExpression(
      final List<ExpressionOperator> children, final String name, final Type outputType) {
    super(children);
    this.name = name;
    this.outputType = outputType;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }

  /**
   * @return the column index of this variable.
   */
  public String getName() {
    return name;
  }
  /**
   * Java substring is empty for python expression.
   */
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return "";
  }
}
