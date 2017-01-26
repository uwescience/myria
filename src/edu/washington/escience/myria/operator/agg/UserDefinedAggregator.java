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
  /** Column indices of this aggregator of the state hash table. */
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
    for (int index = 0; index < emitEvaluators.size(); index++) {
      final GenericEvaluator evaluator = emitEvaluators.get(index);
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
  public void initState(AppendableTable data) {
    Tuple state = new Tuple(data.getSchema().getSubSchema(stateCols));
    initEvaluator.evaluate(null, 0, state, null);
    for (int i = 0; i < stateCols.length; ++i) {
      switch (state.getSchema().getColumnType(i)) {
        case BOOLEAN_TYPE:
          data.putBoolean(stateCols[i], state.getBoolean(i, 0));
          break;
        case INT_TYPE:
          data.putInt(stateCols[i], state.getInt(i, 0));
          break;
        case LONG_TYPE:
          data.putLong(stateCols[i], state.getLong(i, 0));
          break;
        case DOUBLE_TYPE:
          data.putDouble(stateCols[i], state.getDouble(i, 0));
          break;
        case FLOAT_TYPE:
          data.putFloat(stateCols[i], state.getFloat(i, 0));
          break;
        case STRING_TYPE:
          data.putString(stateCols[i], state.getString(i, 0));
          break;
        case DATETIME_TYPE:
          data.putDateTime(stateCols[i], state.getDateTime(i, 0));
          break;
      }
    }
  }
}
