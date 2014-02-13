package edu.washington.escience.myria.expression.evaluate;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;

/**
 * Compile and evaluate expressions.
 */
public abstract class Evaluator {
  /**
   * The expression to be compiled and evaluated.
   */
  private final Expression expression;

  /**
   * The schema of the input tuples to this expression.
   */
  private Schema inputSchema;

  /**
   * The schema of the state.
   */
  private Schema stateSchema;

  /**
   * @param expression the expression for the evaluator
   * @param inputSchema the schema of the input relation
   * @param stateSchema the schema of the state
   */
  public Evaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    this.expression = expression;
    Preconditions.checkNotNull(inputSchema);
    this.inputSchema = inputSchema;
    this.stateSchema = stateSchema;
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
  public void setInputSchema(final Schema inputSchema) {
    this.inputSchema = inputSchema;
    getExpression().resetJavaExpression();
  }

  /**
   * Set the schema of the state.
   * 
   * @param stateSchema schema the schema that the expression expects if it operates on a schema.
   */
  public void setStateSchema(final Schema stateSchema) {
    this.stateSchema = stateSchema;
    getExpression().resetJavaExpression();
  }

  /**
   * @return the type of the output
   */
  public Type getOutputType() {
    return getExpression().getOutputType(getInputSchema(), getStateSchema());
  }

  /**
   * @return the schema of the state
   */
  public Schema getStateSchema() {
    return stateSchema;
  }

  /**
   * @return the {@link #expression}
   */
  public Expression getExpression() {
    return expression;
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  public void compile() throws DbException {
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

  /**
   * @return true if the expression just copies a column from the input
   */
  public boolean isCopyFromInput() {
    final ExpressionOperator rootOp = getExpression().getRootExpressionOperator();
    return rootOp instanceof VariableExpression;
  }

  /**
   * An expression does not have to be compiled when it only renames or copies a column. This is an optimization to
   * avoid evaluating the expression and avoid autoboxing values.
   * 
   * @return true if the expression does not have to be compiled.
   */
  public boolean needsCompiling() {
    return !(isCopyFromInput() || isConstant());
  }

  /**
   * @return true if the expression evaluates to a constant
   */
  public boolean isConstant() {
    return getExpression().isConstant();
  }
}
