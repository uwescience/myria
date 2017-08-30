package edu.washington.escience.myria.expression.evaluate;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply}.
 */
public class GenericEvaluator extends Evaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /** Expression evaluator. */
  private ExpressionEvalInterface evaluator;
  /** The script. */
  private String script;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   * @throws DbException if the compilation failed
   */
  public GenericEvaluator(final Expression expression, final ExpressionOperatorParameter parameters)
      throws DbException {
    super(expression, parameters);
    this.script = getExpression().getJavaExpressionWithAppend(getParameters());
    compile();
  }

  /**
   * @param expression
   * @param script
   * @param parameters
   * @throws DbException if the compilation failed
   */
  public GenericEvaluator(
      final Expression expression,
      final String script,
      final ExpressionOperatorParameter parameters)
      throws DbException {
    super(expression, parameters);
    this.script = script;
    compile();
  }

  /**
   * Compiles the {@link #javaExpression}.
   *
   * @throws DbException if the compilation failed
   */
  @Override
  public void compile() throws DbException {
    IScriptEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception e) {
      LOGGER.error("Could not create expression evaluator", e);
      throw new DbException("Could not create expression evaluator", e);
    }
    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);
    try {
      if (script.contains("append")) {
        evaluator =
            (ExpressionEvalAppendInterface)
                se.createFastEvaluator(
                    script,
                    ExpressionEvalAppendInterface.class,
                    new String[] {
                      Expression.INPUT,
                      Expression.INPUTROW,
                      Expression.STATE,
                      Expression.STATEROW,
                      Expression.STATECOLOFFSET,
                      Expression.RESULT,
                      Expression.COUNT
                    });
      } else {
        evaluator =
            (ExpressionEvalReplaceInterface)
                se.createFastEvaluator(
                    script,
                    ExpressionEvalReplaceInterface.class,
                    new String[] {
                      Expression.INPUT,
                      Expression.INPUTROW,
                      Expression.STATE,
                      Expression.STATEROW,
                      Expression.STATECOLOFFSET
                    });
      }
    } catch (CompileException e) {
      LOGGER.error("Error when compiling expression {}: {}", script, e);
      throw new DbException("Error when compiling expression: " + script, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} since it can evaluate an entire TupleBatch at a time for better locality.
   *
   * @param input a tuple batch
   * @param inputRow index of the row that should be used for input data
   * @param state additional state that affects the computation
   * @param stateRow index of the row that should be used for state
   * @param stateColOffset the column offset of the state
   * @throws DbException in case of error.
   */
  public void updateState(
      @Nonnull final ReadableTable input,
      final int inputRow,
      @Nonnull final MutableTupleBuffer state,
      final int stateRow,
      final int stateColOffset)
      throws DbException {
    ((ExpressionEvalReplaceInterface) evaluator)
        .evaluate(input, inputRow, state, stateRow, stateColOffset);
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} since it can evaluate an entire TupleBatch at a time for better locality.
   *
   * @param input a tuple batch
   * @param inputRow index of the row that should be used for input data
   * @param state additional state that affects the computation
   * @param stateRow index of the row that should be used for state
   * @param result the table storing the result
   * @param count column storing number of results (null for single-valued expressions)
   * @throws DbException in case of error.
   */
  public void eval(
      @Nullable final ReadableTable input,
      final int inputRow,
      @Nullable final ReadableTable state,
      final int stateRow,
      @Nonnull final WritableColumn result,
      @Nullable final WritableColumn count)
      throws DbException {
    Preconditions.checkArgument(
        evaluator != null, "Call compile first or copy the data if it is the same in the input.");
    Preconditions.checkArgument(
        getExpression().isMultiValued() != (count == null),
        "count must be null for a single-valued expression and non-null for a multivalued expression.");
    try {
      ((ExpressionEvalAppendInterface) evaluator)
          .evaluate(input, inputRow, state, stateRow, 0, result, count);
    } catch (Exception e) {
      LOGGER.error(script, e);
      throw e;
    }
  }

  /**
   * Holder class for results and result counts from {@link #evaluateColumn}.
   */
  public static class EvaluatorResult {
    private final ReadableColumn results;
    private final ImmutableList<Column<?>> resultColumns;
    private final ReadableColumn resultCounts;

    protected EvaluatorResult(
        @Nonnull final TupleBuffer results, @Nonnull final Column<?> resultCounts) {
      ImmutableList.Builder<Column<?>> resultColumnsBuilder = ImmutableList.builder();
      for (final TupleBatch tb : results.finalResult()) {
        resultColumnsBuilder.add(tb.getDataColumns().get(0));
      }
      this.resultColumns = resultColumnsBuilder.build();
      this.results = results.asColumn(0);
      this.resultCounts = resultCounts;
    }

    protected EvaluatorResult(
        @Nonnull final Column<?> results, @Nonnull final Column<?> resultCounts) {
      this.resultColumns = ImmutableList.of(results);
      this.results = results;
      this.resultCounts = resultCounts;
    }

    /**
     * @return a {@link ReadableColumn} containing results from {@link #evaluateColumn}
     */
    public ReadableColumn getResults() {
      return results;
    }

    /**
     * @return a {@link List<Column>} containing results from {@link #evaluateColumn}
     */
    public List<Column<?>> getResultColumns() {
      return resultColumns;
    }

    /**
     * @return a {@link Column<Integer>} containing result counts from {@link #evaluateColumn}
     */
    public ReadableColumn getResultCounts() {
      return resultCounts;
    }
  }

  /**
   * Evaluate an expression over an entire TupleBatch and return the column(s) of results, along with a column of result
   * counts from each tuple. This method cannot take state into consideration.
   *
   * @param tb the tuples to be input to this expression
   * @param outputSchema the schema that results from this evaluator belongs to, used to determine the tuple batch size
   * @return an {@link EvaluatorResult} containing the results and result counts of evaluating this expression on the
   *         entire TupleBatch
   * @throws DbException
   */
  public EvaluatorResult evalTupleBatch(final TupleBatch tb, final Schema outputSchema)
      throws DbException {
    final Column<?> constCounts = new ConstantValueColumn(1, Type.INT_TYPE, tb.numTuples());
    int batchSize = TupleUtils.getBatchSize(outputSchema);
    // Critical optimization: return a zero-copy reference to a column referenced by a pure `VariableExpression`.
    if (isCopyFromInput() && batchSize >= tb.numTuples()) {
      ExpressionOperator op = getExpression().getRootExpressionOperator();
      return new EvaluatorResult(
          tb.getDataColumns().get(((VariableExpression) op).getColumnIdx()), constCounts);
    }
    /* For multivalued expressions, we may get more than batchSize results, so we need to pass in a `TupleBuffer` rather
     * than a `ColumnBuilder` to `eval()`, and return a `List<Column>` rather than a `Column` of results. */
    final TupleBuffer resultsBuffer =
        new TupleBuffer(
            Schema.ofFields(getExpression().getOutputName(), getOutputType()), batchSize);
    final WritableColumn resultsWriter = resultsBuffer.asWritableColumn(0);
    // For single-valued expressions, the Java expression will never attempt to write to `countsWriter`.
    WritableColumn countsWriter = null;
    if (getExpression().isMultiValued()) {
      countsWriter = ColumnFactory.allocateColumn(Type.INT_TYPE, batchSize);
    }
    for (int rowIdx = 0; rowIdx < tb.numTuples(); ++rowIdx) {
      /* Hack, tb is either Expression.INPUT or Expression.STATE */
      eval(tb, rowIdx, tb, rowIdx, resultsWriter, countsWriter);
    }
    final Column<?> resultCounts;
    if (getExpression().isMultiValued()) {
      resultCounts = ((ColumnBuilder<?>) countsWriter).build();
    } else {
      resultCounts = constCounts;
    }
    return new EvaluatorResult(resultsBuffer, resultCounts);
  }

  /**
   * @return the script
   */
  public String getScript() {
    return script;
  }
}
