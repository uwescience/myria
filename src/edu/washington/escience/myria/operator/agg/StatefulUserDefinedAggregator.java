/**
 *
 */
package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.expression.evaluate.ScriptEvalInterface;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class StatefulUserDefinedAggregator extends UserDefinedAggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StatefulUserDefinedAggregator.class);
  private List<TupleBatch> ltb;

  /**
   * @param state the initialized state of the tuple
   * @param updateEvaluator updates the state given an input row
   * @param emitEvaluators the evaluators that finalize the state
   * @param resultSchema the schema of the tuples produced by this aggregator
   */

  public StatefulUserDefinedAggregator(final Tuple state, final ScriptEvalInterface updateEvaluator,
      final List<PythonUDFEvaluator> pyUDFEvaluators, final List<GenericEvaluator> emitEvaluators,
      final Schema resultSchema) {
    super(state, updateEvaluator, pyUDFEvaluators, emitEvaluators, resultSchema);
  }

  @Override
  public void add(final ReadableTable from, final Object state) throws DbException {
    // LOGGER.info("add called");
    if (ltb == null) {
      ltb = new ArrayList<>();
    }
    if (!ltb.contains(from)) {
      ltb.add((TupleBatch) from);
    }

    try {
      if (updateEvaluator != null) {
        // this aggreagte has a script eval
        for (int row = 0; row < from.numTuples(); ++row) {
          addRow(from, row, state);
        }
      }
    } catch (Exception e) {
      throw new DbException("Error updating UDA state", e);
    }
  }

  @Override
  public void add(final List<TupleBatch> from, final Object state) throws DbException {
    LOGGER.info("add tuple called");
    ltb = from;

  }

  @Override
  public void addRow(final ReadableTable from, final int row, final Object state) throws DbException {
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
    LOGGER.info("get results called");

    Tuple stateTuple = (Tuple) state;

    // compute results over the tuplebatch list
    if (pyUDFEvaluators.size() > 0) {
      for (int i = 0; i < pyUDFEvaluators.size(); i++) {
        // LOGGER.info("trying to update state variable");
        LOGGER.info("calling evalbatch with tb list size " + ltb.size());
        if (ltb != null && ltb.size() != 0) {
          pyUDFEvaluators.get(i).evalBatch(ltb, stateTuple, stateTuple);
        } else {
          throw new DbException("cannot get results!!");
        }
      }
    }
    // emit results

    for (int index = 0; index < emitEvaluators.size(); index++) {
      final GenericEvaluator evaluator = emitEvaluators.get(index);
      try {
        LOGGER.info("trying to finalize output before emiting");
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

}
