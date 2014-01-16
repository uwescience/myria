package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.expression.VariableExpression;

/**
 * Interface for evaluating janino expressions that expect a state as an argument.
 */
public interface EvaluatorExpectingState extends EvalInterface {
  /**
   * The interface for applying expressions. We only need a reference to the tuple batch and a row id. The variables
   * will be fetched from the tuple buffer using the rowId provided in {@link VariableExpression}.
   * 
   * @param tb a tuple batch
   * @param rowId the row in the tb that should be used.
   * @param state the state that is passed
   * @return the result from the evaluation
   */
  Object evaluate(final TupleBatch tb, final int rowId, Object state);
}
