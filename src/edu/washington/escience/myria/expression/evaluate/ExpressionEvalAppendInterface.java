package edu.washington.escience.myria.expression.evaluate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
   * @param input the input tuple batch, optional
   * @param inputRow row index of the input tuple batch
   * @param state state that is passed during evaluation, optional
   * @param stateRow row index of the state
   * @param result a table storing evaluation results
   * @param count a column storing the number of results returned from this row, optional
   */
  void evaluate(
      @Nullable final ReadableTable input,
      final int inputRow,
      @Nullable final ReadableTable state,
      final int stateRow,
      @Nonnull final WritableColumn result,
      @Nullable final WritableColumn count);
}
