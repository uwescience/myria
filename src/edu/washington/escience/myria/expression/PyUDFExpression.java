package edu.washington.escience.myria.expression;

import java.util.List;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jcraft.jsch.Logger;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.functions.PythonWorker;

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
   * add a counter to flatmap?
   */
  @JsonProperty private final Boolean addCounter;

  /**
   * Python function expression.
   *
   * @param children operand.
   * @param outputType - output Type of the python function
   * @param name - name of the python function in the catalog
   * @param addCounter - add a column id for flatmap py expression.
   */
  public PyUDFExpression(
      final List<ExpressionOperator> children,
      final String name,
      final Type outputType,
      final Boolean addCounter) {
    super(children);
    this.name = name;
    this.outputType = outputType;
    this.addCounter = addCounter;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }
  /**
   * Should add a counter column for flatmap.
   */
  @Override
  public boolean addCounter() {
    return addCounter;
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
