package edu.washington.escience.myria.expression.evaluate;

import java.util.LinkedList;

import com.google.common.collect.Lists;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.StateVariableExpression;
import edu.washington.escience.myria.expression.VariableExpression;

/**
 * 
 */
public class TupleEvaluator extends Evaluator {
  /**
   * True if the input value is the same as the output.
   */
  private final boolean copyFromInput;

  /**
   * True if the expression uses state.
   */
  private final boolean needsState;

  /**
   * @param expression the expression to be evaluated
   * @param inputSchema the schema that the expression expects if it operates on a schema
   * @param stateSchema the schema of the state
   */
  public TupleEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
    ExpressionOperator rootOp = getExpression().getRootExpressionOperator();
    copyFromInput = rootOp instanceof VariableExpression && !(rootOp instanceof StateVariableExpression);
    needsState = hasOperator(StateVariableExpression.class);
  }

  /**
   * @param optype Class to find
   * @return true if the operator is in the expression
   */
  private boolean hasOperator(final Class<StateVariableExpression> optype) {
    LinkedList<ExpressionOperator> ops = Lists.newLinkedList();
    ops.add(getExpression().getRootExpressionOperator());
    while (!ops.isEmpty()) {
      final ExpressionOperator op = ops.pop();
      if (op.getClass().equals(optype)) {
        return true;
      }
      ops.addAll(op.getChildren());
    }
    return false;
  }

  /**
   * @return the copyFromInput
   */
  public boolean isCopyFromInput() {
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

  public boolean needsState() {
    return needsState;
  }
}
