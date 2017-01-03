package edu.washington.escience.myria.expression;

import java.util.List;

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

  /** The name of the python function. */
  @JsonProperty private final String name;

  /**
   * Output Type of python function.
   */
  @JsonProperty private final Type outputType;
  /**
   * is this expression a flatmap?
   */
  @JsonProperty private final Boolean arrayOutputType;

  /**
   * add a counter to flatmap?
   */
  @JsonProperty private final Boolean addCounter;

  /**
   * Python function expression.
   *
   * @param children operand.
   * @param outputType - output Type of the python function
   * @param name - name of the python function in the catalog
   * @param arrayOutputType - is this a flatmap function
   */
  public PyUDFExpression(
      final List<ExpressionOperator> children,
      final String name,
      final Type outputType,
      final Boolean arrayOutputType,
      final Boolean addCounter) {
    super(children);
    this.name = name;
    this.outputType = outputType;
    this.arrayOutputType = arrayOutputType;
    this.addCounter = addCounter;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }

  @Override
  public boolean addCounter() {
    return addCounter;
  }

  @Override
  public boolean hasArrayOutputType() {
    return arrayOutputType;
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
