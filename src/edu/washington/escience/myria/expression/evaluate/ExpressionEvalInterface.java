package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Interface for evaluating a single {@link edu.washington.escience.myria.expression.Expression} and appending the
 * results to a column, along with a count of results.
 */
public interface ExpressionEvalInterface {
  /**
   * The interface evaluating a single {@link edu.washington.escience.myria.expression.Expression} and appending it to a
   * column. We only need a reference to the tuple batch and a row id, plus the optional state of e.g. an
   * {@link edu.washington.escience.myria.operator.agg.Aggregate} or a
   * {@link edu.washington.escience.myria.operator.StatefulApply}. The variables will be fetched from the tuple buffer
   * using the rowId provided in {@link edu.washington.escience.myria.expression.VariableExpression}.
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
