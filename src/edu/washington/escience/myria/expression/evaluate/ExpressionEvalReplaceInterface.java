package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface for evaluating a single {@link edu.washington.escience.myria.expression.Expression} and appending the
 * results to a column, along with a count of results.
 */
public interface ExpressionEvalReplaceInterface extends ExpressionEvalInterface {
  /**
   * The interface evaluating a single {@link edu.washington.escience.myria.expression.Expression} and replace old
   * values in a state column with the results.
   *
   * @param input the input tuple batch
   * @param inputRow row index of the input tuple batch
   * @param state optional state that is passed during evaluation
   * @param stateRow row index of the state
   * @param stateColOffset column offset of the state
   */
  void evaluate(
      final ReadableTable input,
      final int inputRow,
      final MutableTupleBuffer state,
      final int stateRow,
      final int stateColOffset);
}
