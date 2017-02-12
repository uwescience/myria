package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.Schema;
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
  private final ScriptEvalInterface initEvaluator;
  /** Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updateExpressions}. */
  private final ScriptEvalInterface updateEvaluator;
  /** The Schema of the tuples produced by this aggregator. */
  private final Schema resultSchema;
  /** The Schema of the state. */
  private final Schema stateSchema;

  /**
   * @param initEvaluator initialize the state
   * @param updateEvaluator updates the state given an input row
   * @param resultSchema the schema of the tuples produced by this aggregator
   * @param stateSchema the schema of the state
   */
  public UserDefinedAggregator(
      final ScriptEvalInterface initEvaluator,
      final ScriptEvalInterface updateEvaluator,
      final Schema resultSchema,
      final Schema stateSchema) {
    this.initEvaluator = initEvaluator;
    this.updateEvaluator = updateEvaluator;
    this.stateSchema = stateSchema;
    this.resultSchema = resultSchema;
  }

  @Override
  public int getStateSize() {
    return stateSchema.numColumns();
  }

  @Override
  public void addRow(
      TupleBatch input, int inputRow, MutableTupleBuffer state, int stateRow, final int offset) {
    updateEvaluator.evaluate(input, inputRow, state, stateRow, offset);
  }

  @Override
  public void initState(final MutableTupleBuffer state, final int offset) {
    initEvaluator.evaluate(null, 0, state, 0, offset);
  }
}
