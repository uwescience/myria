package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * 
 */
public class TupleEvaluator extends Evaluator {
  /**
   * The schema of the input tuples to this expression.
   */
  private Schema inputSchema;

  /**
   * True if the input value is the same as the output.
   */
  private final boolean copyFromInput;

  /**
   * @param expression the expression to be evaluated
   * @param inputSchema the schema that the expression expects if it operates on a schema
   */
  public TupleEvaluator(final Expression expression, final Schema inputSchema) {
    super(expression);

    Preconditions.checkNotNull(inputSchema);
    this.inputSchema = inputSchema;

    copyFromInput = getExpression().getRootExpressionOperator() instanceof VariableExpression;
  }

  /**
   * @return the inputSchema
   */
  protected Schema getInputSchema() {
    return inputSchema;
  }

  /**
   * Set the schema of the input tuples to this expression.
   * 
   * @param inputSchema schema the schema that the expression expects if it operates on a schema.
   */
  public void setSchema(final Schema inputSchema) {
    this.inputSchema = inputSchema;
    getExpression().resetJavaExpression();
  }

  /**
   * @return the copyFromInput
   */
  protected boolean isCopyFromInput() {
    return copyFromInput;
  }

  /**
   * An expression does not have to be compiled when it only renames or copies a column. This is an optimization to
   * avoid evaluating the expression and avoid autoboxing values.
   * 
   * @return true if the expression does not have to be compiled.
   */
  public boolean needsCompiling() {
    return !copyFromInput;
  }

  /**
   * @return the Java form of this expression.
   */
  public String getJavaExpression() {
    return getExpression().getJavaExpression(getInputSchema());
  }

  /**
   * @return the type of the output
   */
  public Type getOutputType() {
    return getExpression().getOutputType(getInputSchema());
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return getExpression().getOutputName();
  }
}
