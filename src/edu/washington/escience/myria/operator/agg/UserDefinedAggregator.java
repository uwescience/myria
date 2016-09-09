package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.expression.evaluate.ScriptEvalInterface;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregator implements Aggregator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UserDefinedAggregator.class);

  /**
   * The state of the aggregate variables.
   */
  protected final Tuple initialState;
  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}.
   */
  protected final ScriptEvalInterface updateEvaluator;
  /**
   * One evaluator for each expression in {@link #emitExpressions}.
   */
  protected final List<GenericEvaluator> emitEvaluators;
  protected final List<PythonUDFEvaluator> pyUDFEvaluators;

  /**
   * The Schema of the tuples produced by this aggregator.
   */
  protected final Schema resultSchema;
  // private final List<Integer> needsPyEvaluator;

  /**
   * @param state the initialized state of the tuple
   * @param updateEvaluator updates the state given an input row
   * @param emitEvaluators the evaluators that finalize the state
   * @param resultSchema the schema of the tuples produced by this aggregator
   */
  public UserDefinedAggregator(final Tuple state, final ScriptEvalInterface updateEvaluator,
      final List<PythonUDFEvaluator> pyUDFEvaluators, final List<GenericEvaluator> emitEvaluators,
      final Schema resultSchema) {
    initialState = state;
    this.updateEvaluator = updateEvaluator;
    this.emitEvaluators = emitEvaluators;
    this.pyUDFEvaluators = pyUDFEvaluators;
    this.resultSchema = resultSchema;

  }

  @Override
  public void add(final ReadableTable from, final Object state) throws DbException {
    for (int row = 0; row < from.numTuples(); ++row) {
      addRow(from, row, state);
    }
  }

  @Override
  public void addRow(final ReadableTable from, final int row, final Object state) throws DbException {

    if (pyUDFEvaluators.size() > 0) {
      LOGGER.info("this aggregate has python UDF, StatefulAggreagte should be called!");
      throw new DbException("this aggregate has python UDF, StatefulAggreagte should be called!");
    }

    Tuple stateTuple = (Tuple) state;

    try {
      if (updateEvaluator != null) {
        updateEvaluator.evaluate(from, row, stateTuple, stateTuple);
      }

    } catch (Exception e) {
      // LOGGER.error("Error updating UDA state", e);
      throw new DbException("Error updating UDA state", e);
    }
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state) throws DbException,
      IOException {
    Tuple stateTuple = (Tuple) state;
    for (int index = 0; index < emitEvaluators.size(); index++) {
      final GenericEvaluator evaluator = emitEvaluators.get(index);
      try {
        evaluator.eval(null, 0, dest.asWritableColumn(destColumn + index), stateTuple);
      } catch (InvocationTargetException e) {
        throw new DbException("Error finalizing aggregate", e);
      }
    }
  }

  @Override
  public Schema getResultSchema() {
    return resultSchema;
  }

  @Override
  public Object getInitialState() {

    return initialState.clone();
  }

  @Override
  public void add(final List<Object> from, final Object state) throws DbException {
    throw new DbException(" method not implemented");
  }
}
