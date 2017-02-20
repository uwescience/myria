package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface for evaluating a single {@link edu.washington.escience.myria.expression.Expression} and appending the
 * results to a column, along with a count of results.
 */
public interface ExpressionEvalAppendInterface extends ExpressionEvalInterface {
  /**
   * The interface evaluates a single {@link edu.washington.escience.myria.expression.Expression} and appends the
   * results and (optional) counts to the given columns.
   *
   * @param input the input tuple batch
   * @param inputRow row index of the input tuple batch
   * @param state optional state that is passed during evaluation
   * @param stateRow row index of the state
   * @param result a table storing evaluation results
   * @param count a column storing the number of results returned from this row
   */
  void evaluate(
      final ReadableTable input,
      final int inputRow,
      final ReadableTable state,
      final int stateRow,
      final WritableColumn result,
      final WritableColumn count);
}
