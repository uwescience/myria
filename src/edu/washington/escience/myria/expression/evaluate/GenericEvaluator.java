package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import javax.annotation.Nonnull;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

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
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBuffer;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply}.
 */
public class GenericEvaluator extends Evaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * Expression evaluator.
   */
  private ExpressionEvalInterface evaluator;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   */
  public GenericEvaluator(
      final Expression expression, final ExpressionOperatorParameter parameters) {
    super(expression, parameters);
  }

  /**
   * Compiles the {@link #javaExpression}.
   *
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    Preconditions.checkArgument(
        needsCompiling() || (getStateSchema() != null),
        "This expression does not need to be compiled.");

    String javaExpression = getJavaExpressionWithAppend();
    IScriptEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception e) {
      LOGGER.error("Could not create expression evaluator", e);
      throw new DbException("Could not create expression evaluator", e);
    }

    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

    try {
      evaluator =
          (ExpressionEvalInterface)
              se.createFastEvaluator(
                  javaExpression,
                  ExpressionEvalInterface.class,
                  new String[] {
                    Expression.TB,
                    Expression.ROW,
                    Expression.COUNT,
                    Expression.RESULT,
                    Expression.STATE
                  });
    } catch (CompileException e) {
      LOGGER.error("Error when compiling expression {}: {}", javaExpression, e);
      throw new DbException("Error when compiling expression: " + javaExpression, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} since it can evaluate an entire TupleBatch at a time for better locality.
   *
   * @param tb a tuple batch
   * @param rowIdx index of the row that should be used for input data
   * @param count column storing number of results
   * @param result the table storing the result
   * @param state additional state that affects the computation
   * @throws InvocationTargetException exception thrown from janino
   */
  public void eval(
      final ReadableTable tb,
      final int rowIdx,
      final WritableColumn count,
      final WritableColumn result,
      final ReadableTable state)
      throws InvocationTargetException {
    Preconditions.checkArgument(
        evaluator != null, "Call compile first or copy the data if it is the same in the input.");
    try {
      evaluator.evaluate(tb, rowIdx, count, result, state);
    } catch (Exception e) {
      LOGGER.error(getJavaExpressionWithAppend(), e);
      throw e;
    }
  }

  /**
   * @return the Java form of this expression.
   */
  @Override
  public String getJavaExpressionWithAppend() {
    return getExpression().getJavaExpressionWithAppend(getParameters());
  }

  /**
   * Holder class for results and result counts from {@link #evaluateColumn}.
   */
  public static class EvaluatorResult {
    private final ReadableColumn results;
    private final Column<?> resultCounts;

    protected EvaluatorResult(
        @Nonnull final ReadableColumn results, @Nonnull final Column<?> resultCounts) {
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
     * This is useful for, e.g., constructing a {@link TupleBatch} from several evaluators.
     * This will return null if the result was not generated from a single-valued expression.
     *
     * @return a {@link Column<?>} containing results from {@link #evaluateColumn}
     */
    public Column<?> getResultsAsColumn() {
      if (results instanceof Column<?>) {
        return (Column<?>) results;
      } else {
        return null;
      }
    }
    /**
     * @return a {@link Column<Integer>} containing result counts from {@link #evaluateColumn}
     */
    public Column<?> getResultCounts() {
      return resultCounts;
    }
  }

  /**
   * Evaluate an expression over an entire TupleBatch and return the column of results. This method cannot take state
   * into consideration.
   *
   * @param tb the tuples to be input to this expression
   * @return an {@link EvaluatorResult} containing the results and result counts of evaluating this expression on the entire TupleBatch
   * @throws InvocationTargetException exception thrown from janino
   */
  public EvaluatorResult evaluateColumn(final TupleBatch tb) throws InvocationTargetException {
    // Optimization for result counts of single-valued expressions.
    final Column<?> constCounts = new ConstantValueColumn(1, Type.INT_TYPE, tb.numTuples());
    final WritableColumn countsWriter;
    if (getExpression().isMultivalued()) {
      countsWriter = ColumnFactory.allocateColumn(Type.INT_TYPE);
    } else {
      // For single-valued expressions, the Java expression will never attempt to write to `countsWriter`.
      countsWriter = null;
    }
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    // Critical optimization: return a zero-copy reference to a column referenced by a pure `VariableExpression`.
    if (isCopyFromInput()) {
      return new EvaluatorResult(
          tb.getDataColumns().get(((VariableExpression) op).getColumnIdx()), constCounts);
    }
    // For multivalued expressions, we may get more than `TupleBatch.BATCH_SIZE` results,
    // so we need to allocate a `TupleBuffer` instead of a `ColumnBuilder` for results.
    // (That is why we need `ReadableColumn` rather than `Column` as the result type.)
    final WritableColumn resultsWriter;
    final Type type = getOutputType();
    TupleBuffer resultsBuffer = null;
    if (getExpression().isMultivalued()) {
      resultsBuffer = new TupleBuffer(Schema.ofFields(getExpression().getOutputName(), type));
      resultsWriter = resultsBuffer.asWritableColumn(0);
    } else {
      // For single-valued expressions, return a `Column` to allow callers to easily construct a `TupleBatch`.
      resultsWriter = ColumnFactory.allocateColumn(type);
    }
    for (int rowIdx = 0; rowIdx < tb.numTuples(); ++rowIdx) {
      eval(tb, rowIdx, countsWriter, resultsWriter, null);
    }
    final Column<?> resultCounts;
    final ReadableColumn results;
    if (getExpression().isMultivalued()) {
      resultCounts = ((ColumnBuilder<?>) countsWriter).build();
      results = resultsBuffer.asColumn(0);
    } else {
      resultCounts = constCounts;
      results = ((ColumnBuilder<?>) resultsWriter).build();
    }
    return new EvaluatorResult(results, resultCounts);
  }
}
