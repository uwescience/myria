package edu.washington.escience.myria.operator.agg;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregator implements Aggregator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The state of the aggregate variables.
   */
  private Tuple state;
  /**
   * The old state of the aggregate variables.
   */
  private Tuple oldState;
  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}.
   */
  private final ArrayList<GenericEvaluator> updateEvaluators;
  /**
   * One evaluator for each expression in {@link #emitExpressions}.
   */
  private final ArrayList<GenericEvaluator> emitEvaluators;
  /**
   * The Schema of the tuples produced by this aggregator.
   */
  private final Schema resultSchema;

  /**
   * @param state the initialized state of the tuple
   * @param updateEvaluators the evaluators that update the state given an input row
   * @param emitEvaluators the evaluators that finalize the state
   * @param resultSchema the schema of the tuples produced by this aggregator
   */
  public UserDefinedAggregator(final Tuple state, final ArrayList<GenericEvaluator> updateEvaluators,
      final ArrayList<GenericEvaluator> emitEvaluators, final Schema resultSchema) {
    this.state = state;
    oldState = state.clone();
    this.updateEvaluators = updateEvaluators;
    this.emitEvaluators = emitEvaluators;
    this.resultSchema = resultSchema;
  }

  @Override
  public void add(final ReadableTable from) throws DbException {
    for (int row = 0; row < from.numTuples(); ++row) {
      addRow(from, row);
    }
  }

  @Override
  public void addRow(final ReadableTable from, final int row) throws DbException {
    // set the old state to the current state
    Tuple tmp = state;
    state = oldState;
    oldState = tmp;
    // update state
    for (int columnIdx = 0; columnIdx < tmp.getSchema().numColumns(); columnIdx++) {
      try {
        updateEvaluators.get(columnIdx).eval(from, row, state.getColumn(columnIdx), oldState);
      } catch (InvocationTargetException e) {
        throw new DbException("Error updating state", e);
      }
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) throws DbException {
    for (int index = 0; index < emitEvaluators.size(); index++) {
      final GenericEvaluator evaluator = emitEvaluators.get(index);
      try {
        evaluator.eval(null, 0, dest.asWritableColumn(destColumn + index), state);
      } catch (InvocationTargetException e) {
        throw new DbException("Error finalizing aggregate", e);
      }
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }
}
