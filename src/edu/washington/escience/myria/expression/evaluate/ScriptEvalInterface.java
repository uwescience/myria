package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;

/**
 * Interface for evaluators that take multiple expressions and may write multiple columns.
 */
public interface ScriptEvalInterface {
  /**
   * The interface for applying expressions. We only need a reference to the tuple batch and a row id. The variables
   * will be fetched from the tuple buffer using the rowId provided in
   * {@link edu.washington.escience.myria.expression.VariableExpression} or
   * {@link edu.washington.escience.myria.expression.StateExpression}.
   *
   * @param tb a tuple batch
   * @param rowId the row in the tb that should be used.
   * @param result where the output should be written.
   * @param state state that is passed during evaluation, and written after the new state is computed.
   */
  void evaluate(
      final ReadableTable tb, final int rowId, final AppendableTable result, final Tuple state);
}
