package edu.washington.escience.myria.expression.evaluate;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface that evaluates a single {@link edu.washington.escience.myria.expression.Expression} on a row from an input
 * TupleBatch and overwrites a row in a state TupleBatch with the results.
 */
public interface ExpressionEvalReplaceInterface extends ExpressionEvalInterface {
  /**
   * The interface evaluating a single {@link edu.washington.escience.myria.expression.Expression} and replace old
   * values in a state column with the results.
   *
   * @param input the input tuple batch
   * @param inputRow row index of the input tuple batch
   * @param state state that is passed during evaluation
   * @param stateRow row index of the state
   * @param stateColOffset column offset of the state
   */
  void evaluate(
      @Nonnull final ReadableTable input,
      final int inputRow,
      @Nonnull final MutableTupleBuffer state,
      final int stateRow,
      final int stateColOffset);
}
