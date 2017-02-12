package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface for evaluators that take multiple expressions and may write multiple columns.
 */
public interface ScriptEvalInterface {
  /**
   * The interface for applying expressions. The variables will be fetched from the tuple buffer using the rowId
   * provided in {@link edu.washington.escience.myria.expression.VariableExpression} or
   * {@link edu.washington.escience.myria.expression.StateExpression}.
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
