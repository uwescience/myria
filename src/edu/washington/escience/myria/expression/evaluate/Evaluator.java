package edu.washington.escience.myria.expression.evaluate;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.StateExpression;
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
   * Parameters passed for creating the java expression and getting the output type.
   */
  private final ExpressionOperatorParameter parameters;

  /**
   * True if the expression uses state.
   */
  private final boolean needsState;

  /**
   * @param expression the expression to be evaluated
   * @param parameters parameters that are passed to the expression
   */
  public Evaluator(final Expression expression, final ExpressionOperatorParameter parameters) {
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.parameters = Preconditions.checkNotNull(parameters, "parameters");
    if (getExpression().hasOperator(VariableExpression.class)) {
      Preconditions.checkNotNull(
          parameters.getSchema(), "ExpressionOperatorParameter input schema");
    }
    needsState = getExpression().hasOperator(StateExpression.class);
    if (needsState) {
      Preconditions.checkNotNull(
          parameters.getStateSchema(), "ExpressionOperatorParameter state schema");
    }
  }

  /**
   * @return the inputSchema
   */
  protected Schema getInputSchema() {
    return parameters.getSchema();
  }

  /**
   * @return the schema of the state
   */
  public Schema getStateSchema() {
    return parameters.getStateSchema();
  }

  /**
   * @return the parameters that are passed down the expression tree
   */
  public ExpressionOperatorParameter getParameters() {
    return parameters;
  }

  /**
   * @return the type of the output
   */
  public Type getOutputType() {
    return getExpression().getOutputType(parameters);
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
  public void compile() throws DbException {}

  /**
   * @return the Java form of this expression, including appending to the result.
   */
  public String getJavaExpressionWithAppend() {
    return getExpression().getJavaExpression(parameters);
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

  /**
   * @return true if the expression accesses the state.
   */
  public boolean needsState() {
    return needsState;
  }
}
