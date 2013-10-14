package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.TupleBatch;

/**
 * Interface for evaluating janino expressions.
 */
public interface Evaluator {
  /**
   * The interface for applying expressions. We only need a reference to the tuple batch and a row id. The variables
   * will be fetched from the tuple buffer using the rowId provided in {@link VariableExpression}.
   * 
   * @param tb a tuple batch
   * @param rowId the row in the tb that should be used.
   * @return the result from the evaluation
   */
  Object evaluate(final TupleBatch tb, final int rowId);
}
