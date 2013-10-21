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
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;

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
   * An expression does not have to be compiled when it only renames or copies a column. This is an optimization to
   * avoid evaluating the expression and avoid autoboxing values.
   */
  private final boolean copyFromInput;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private Expression() {
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
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  public void compile() throws DbException {
    Preconditions.checkArgument(!copyFromInput,
        "This expression does not need to be compiled because the data can be copied from the input.");
    javaExpression = rootExpressionOperator.getJavaString(Objects.requireNonNull(inputSchema));

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator = (Evaluator) se.createFastEvaluator(javaExpression, Evaluator.class, new String[] { "tb", "rowId" });
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the expression using the {@link #evaluator}. Prefer to use
   * {@link #evalAndPut(TupleBatch, int, TupleBatchBuffer, int)} as it can copy data without evaluating the expression.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
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

  /**
   * Often, there is no need to compile this expression because the input value is the same as the output.
   * 
   * @return true if the expression does not have to be compiled.
   */
  public boolean needsCompiling() {
    return !copyFromInput;
  }

  /**
   * Runs {@link #eval(TupleBatch, int)} if necessary and puts the result in the target tuple buffer.
   * 
   * If evaluating is not necessary, the data is copied directly from the source tuple batch into the target buffer.
   * 
   * @param sourceTupleBatch the tuple buffer that should be used as input
   * @param sourceRowIdx the row that should be used in the input batch
   * @param targetTupleBuffer the tuple buffer that should be used as output
   * @param targetColumnIdx the column that the data should be written to
   * @throws InvocationTargetException exception thrown from janino
   */
  @SuppressWarnings("deprecation")
  public void evalAndPut(final TupleBatch sourceTupleBatch, final int sourceRowIdx,
      final TupleBatchBuffer targetTupleBuffer, final int targetColumnIdx) throws InvocationTargetException {
    if (copyFromInput) {
      final Column<?> sourceColumn =
          sourceTupleBatch.getDataColumns().get(((VariableExpression) rootExpressionOperator).getColumnIdx());
      targetTupleBuffer.put(targetColumnIdx, sourceColumn, sourceRowIdx);
    } else {
      Object result = eval(sourceTupleBatch, sourceRowIdx);
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      targetTupleBuffer.put(targetColumnIdx, result);
    }

  }
}
