package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.List;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregator implements Aggregator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(UserDefinedAggregator.class);

  /** Evaluators that initialize the state. */
  protected final List<GenericEvaluator> initEvaluators;
  /** Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}. */
  protected final List<GenericEvaluator> updateEvaluators;
  /** The Schema of the state. */
  private final Schema stateSchema;

  /**
   * @param initEvaluators initialize the state
   * @param updateEvaluators updates the state given an input row
   * @param pyUpdateEvaluators for python expression evaluation.
   * @param emitEvaluators the evaluators that finalize the state
   * @param resultSchema the schema of the tuples produced by this aggregator
   * @param stateSchema the schema of the state
   */
  public UserDefinedAggregator(
      final List<GenericEvaluator> initEvaluators,
      final List<GenericEvaluator> updateEvaluators,
      final Schema resultSchema,
      final Schema stateSchema) {
    this.initEvaluators = initEvaluators;
    this.updateEvaluators = updateEvaluators;
    this.stateSchema = stateSchema;
  }

  @Override
  public int getStateSize() {
    return stateSchema.numColumns();
  }

  @Override
  public void addRow(
      TupleBatch input, int inputRow, MutableTupleBuffer state, int stateRow, final int offset)
      throws DbException {
    for (GenericEvaluator eval : updateEvaluators) {
      eval.updateState(input, inputRow, state, stateRow, offset);
    }
  }

  @Override
  public void initState(final MutableTupleBuffer state, final int offset) throws DbException {
    for (GenericEvaluator eval : initEvaluators) {
      eval.updateState(null, 0, state, 0, offset);
    }
  }

  /**
   * @param tb
   * @param offset
   * @throws DbException
 * @throws IOException 
 * @throws BufferOverflowException 
   */
  public void finalizePythonUpdaters(final MutableTupleBuffer tb, final int offset)
      throws DbException, BufferOverflowException, IOException {
    for (int i = 0; i < updateEvaluators.size(); ++i) {
      GenericEvaluator eval = updateEvaluators.get(i);
      if (eval instanceof PythonUDFEvaluator) {
        ((PythonUDFEvaluator) eval).evalGroups(tb, offset + i);
      }
    }
  }
}
