package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.ScriptEvalInterface;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaArrayUtils;

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
  /** One evaluator for each expression in {@link #emitExpressions}. */
  private final List<GenericEvaluator> emitEvaluators;
  /** The Schema of the tuples produced by this aggregator. */
  private final Schema resultSchema;
  /** The Schema of the state. */
  private final Schema stateSchema;
  /** Column indices of this aggregator in the state hash table. */
  private final int[] stateCols;

  /**
   * @param initEvaluator initialize the state
   * @param updateEvaluator updates the state given an input row
   * @param emitEvaluators the evaluators that finalize the state
   * @param resultSchema the schema of the tuples produced by this aggregator
   * @param stateSchema the schema of the state
   * @param offset the starting column index of aggregators made by this factory in the state hash table
   */
  public UserDefinedAggregator(
      final ScriptEvalInterface initEvaluator,
      final ScriptEvalInterface updateEvaluator,
      final List<GenericEvaluator> emitEvaluators,
      final Schema resultSchema,
      final Schema stateSchema,
      final int offset) {
    this.initEvaluator = initEvaluator;
    this.updateEvaluator = updateEvaluator;
    this.emitEvaluators = emitEvaluators;
    this.resultSchema = resultSchema;
    this.stateSchema = stateSchema;
    this.stateCols = MyriaArrayUtils.range(offset, stateSchema.numColumns());
  }

  @Override
  public List<Column<?>> emitOutput(final TupleBatch tb) {
    List<Column<?>> ret = new ArrayList<Column<?>>();
    TupleBatch state =
        tb.selectColumns(MyriaArrayUtils.range(stateCols[0], stateSchema.numColumns()));
    for (final GenericEvaluator evaluator : emitEvaluators) {
      ColumnBuilder<?> col = ColumnBuilder.of(evaluator.getOutputType());
      evaluator.eval(null, 0, null, col, state);
      ret.add(col.build());
    }
    return ret;
  }

  @Override
  public Schema getOutputSchema() {
    return resultSchema;
  }

  @Override
  public void addRow(TupleBatch from, int fromRow, MutableTupleBuffer to, int toRow) {
    Tuple input = new Tuple(to, toRow, stateCols);
    Tuple output = new Tuple(input.getSchema());
    updateEvaluator.evaluate(from, fromRow, output, input);
    for (int i = 0; i < stateCols.length; ++i) {
      to.replace(stateCols[i], toRow, output.asColumn(i), 0);
    }
  }

  @Override
  public void initState(AppendableTable state) {
    Tuple stateTuple = new Tuple(state.getSchema().getSubSchema(stateCols));
    initEvaluator.evaluate(null, 0, stateTuple, null);
    for (int i = 0; i < stateCols.length; ++i) {
      switch (stateTuple.getSchema().getColumnType(i)) {
        case BOOLEAN_TYPE:
          state.putBoolean(stateCols[i], stateTuple.getBoolean(i, 0));
          break;
        case INT_TYPE:
          state.putInt(stateCols[i], stateTuple.getInt(i, 0));
          break;
        case LONG_TYPE:
          state.putLong(stateCols[i], stateTuple.getLong(i, 0));
          break;
        case DOUBLE_TYPE:
          state.putDouble(stateCols[i], stateTuple.getDouble(i, 0));
          break;
        case FLOAT_TYPE:
          state.putFloat(stateCols[i], stateTuple.getFloat(i, 0));
          break;
        case STRING_TYPE:
          state.putString(stateCols[i], stateTuple.getString(i, 0));
          break;
        case DATETIME_TYPE:
          state.putDateTime(stateCols[i], stateTuple.getDateTime(i, 0));
          break;
      }
    }
  }
}
