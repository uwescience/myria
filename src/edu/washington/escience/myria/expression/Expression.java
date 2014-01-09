package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * An expression that can be applied to a tuple.
 */
public class Expression implements Serializable {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Name of the column that the result will be written to.
   */
  @JsonProperty
  private final String outputName;

  /**
   * The java expression to be evaluated.
   */
  @JsonProperty
  private String javaExpression;
  /**
   * Expression encoding reference is needed to get the output type.
   */

  @JsonProperty
  private final ExpressionOperator rootExpressionOperator;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public Expression() {
    outputName = null;
    rootExpressionOperator = null;
  }

  /**
   * Constructs the Expression object.
   * 
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    outputName = null;
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    this.outputName = outputName;
  }

  /**
   * @return the rootExpressionOperator
   */
  protected ExpressionOperator getRootExpressionOperator() {
    return rootExpressionOperator;
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }

  /**
   * @param inputSchema the schema of the input relation
   * @return the Java form of this expression.
   */
  public String getJavaExpression(final Schema inputSchema) {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(Objects.requireNonNull(inputSchema));
    }
    return javaExpression;
  }

  /**
   * @param inputSchema the schema of the input relation
   * @return the type of the output
   */
  public Type getOutputType(final Schema inputSchema) {
    return rootExpressionOperator.getOutputType(Objects.requireNonNull(inputSchema));
  }

  /**
   * @return the Java form of this expression.
   */
  public String getJavaExpression() {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(null);
    }
    return javaExpression;
  }

  /**
   * @return the type of the output
   */
  public Type getOutputType() {
    return rootExpressionOperator.getOutputType(null);
  }

  /**
   * Reset {@link #javaExpression}.
   */
  public void resetJavaExpression() {
    javaExpression = null;
  }
}