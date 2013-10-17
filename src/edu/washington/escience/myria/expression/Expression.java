package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
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
   * The java expression to be evaluated in {@link #eval}.
   */
  @JsonProperty
  private String javaExpression;

  /**
   * Expression encoding reference is needed to get the output type.
   */
  @JsonProperty
  private final ExpressionOperator rootExpressionOperator;

  /**
   * Expression evaluator.
   */
  private Evaluator evaluator;

  /**
   * The schema of the input tuples to this expression.
   */
  private Schema inputSchema;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private Expression() {
    outputName = null;
    rootExpressionOperator = null;
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
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   * @param inputSchema the schema of the input tuples to this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator, final Schema inputSchema) {
    this.outputName = outputName;
    this.rootExpressionOperator = rootExpressionOperator;
    this.inputSchema = inputSchema;
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  public void compile() throws DbException {
    javaExpression = rootExpressionOperator.getJavaString(Objects.requireNonNull(inputSchema));

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator = (Evaluator) se.createFastEvaluator(javaExpression, Evaluator.class, new String[] { "tb", "rowId" });
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the expression using the {@link #evaluator}.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null, "Call compile first.");
    return evaluator.evaluate(tb, rowId);
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
    evaluator = null;
  }
}
