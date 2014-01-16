package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;

/**
 * 
 */
public class TupleEvaluator extends Evaluator {
  /**
   * True if the input value is the same as the output.
   */
  private final boolean copyFromInput;

  /**
   * @param expression the expression to be evaluated
   * @param inputSchema the schema that the expression expects if it operates on a schema
   * @param stateSchema the schema of the state
   */
  public TupleEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
    copyFromInput = getExpression().getRootExpressionOperator() instanceof VariableExpression;
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
    return getExpression().getJavaExpression(getInputSchema(), getStateSchema());
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return getExpression().getOutputName();
  }
}
