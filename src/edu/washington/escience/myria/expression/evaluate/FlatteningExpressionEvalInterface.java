package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface for evaluating a single {@link edu.washington.escience.myria.expression.Expression} and returning an
 * Iterable of results.
 */
public interface FlatteningExpressionEvalInterface<T extends Type> {
  /**
   * The interface evaluating a single {@link edu.washington.escience.myria.expression.Expression} and appending it to a
   * column. We only need a reference to the tuple batch and a row id, plus the optional state of e.g. an
   * {@link edu.washington.escience.myria.operator.agg.Aggregate} or a
   * {@link edu.washington.escience.myria.operator.StatefulApply}. The variables will be fetched from the tuple buffer
   * using the rowId provided in {@link edu.washington.escience.myria.expression.VariableExpression}.
   *
   * @param tb a tuple batch
   * @param rowId the row in the tb that should be used.
   * @param state optional state that is passed during evaluation
   */
  Iterable<T> evaluate(final ReadableTable tb, final int rowId, final ReadableTable state);
}
