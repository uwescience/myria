package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * An expression that can be applied to a tuple.
 */
public abstract class Expression implements Serializable {

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
   * The schema of the input tuples to this expression.
   */
  private Schema inputSchema;
  /**
   * An expression does not have to be compiled when it only renames or copies a column. This is an optimization to
   * avoid evaluating the expression and avoid autoboxing values.
   */
  private final boolean copyFromInput;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public Expression() {
    outputName = null;
    rootExpressionOperator = null;
    copyFromInput = false;
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator) {
    this.outputName = outputName;
    this.rootExpressionOperator = rootExpressionOperator;
    if (rootExpressionOperator instanceof VariableExpression) {
      copyFromInput = true;
    } else {
      copyFromInput = false;
    }
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   * @param inputSchema the schema of the input tuples to this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator, final Schema inputSchema) {
    this(outputName, rootExpressionOperator);
    this.inputSchema = inputSchema;
  }

  /**
   * @return the rootExpressionOperator
   */
  protected ExpressionOperator getRootExpressionOperator() {
    return rootExpressionOperator;
  }

  /**
   * @return the inputSchema
   */
  protected Schema getInputSchema() {
    return inputSchema;
  }

  /**
   * @return the copyFromInput
   */
  protected boolean isCopyFromInput() {
    return copyFromInput;
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }

  /**
   * @return the Java form of this expression.
   */
  @JsonProperty
  public String getJavaExpression() {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(Objects.requireNonNull(inputSchema));
    }
    return javaExpression;
  }

  /**
   * @param javaExpression the javaExpression to set
   */
  public void setJavaExpression(final String javaExpression) {
    this.javaExpression = javaExpression;
  }

  /**
   * @return the type of the output
   */
  public Type getOutputType() {
    return rootExpressionOperator.getOutputType(Objects.requireNonNull(inputSchema));
  }

  @Override
  public String toString() {
    return getJavaExpression();
  }

  /**
   * Set the schema of the input tuples to this expression.
   * 
   * @param inputSchema the schema of the input tuples to this expression.
   */
  public void setSchema(final Schema inputSchema) {
    this.inputSchema = inputSchema;
    javaExpression = null;
  }

  /**
   * Often, there is no need to compile this expression because the input value is the same as the output.
   * 
   * @return true if the expression does not have to be compiled.
   */
  public boolean needsCompiling() {
    return !copyFromInput;
  }

}