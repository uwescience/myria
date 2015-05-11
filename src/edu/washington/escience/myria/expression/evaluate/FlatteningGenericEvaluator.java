package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply} and {@link StatefulApply}.
 */
public class FlatteningGenericEvaluator extends Evaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * Expression evaluator.
   */
  private FlatteningExpressionEvalInterface evaluator;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   */
  public FlatteningGenericEvaluator(final Expression expression, final ExpressionOperatorParameter parameters) {
    super(expression, parameters);
  }

  /**
   * Compiles the {@link #javaExpression}.
   *
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    Preconditions.checkArgument(needsCompiling() || (getStateSchema() != null),
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
          (FlatteningExpressionEvalInterface) se.createFastEvaluator(javaExpression,
              FlatteningExpressionEvalInterface.class, new String[] {
                  Expression.TB, Expression.ROW, Expression.COUNT, Expression.RESULT, Expression.COL });
    } catch (CompileException e) {
      LOGGER.error("Error when compiling expression {}: {}", javaExpression, e);
      throw new DbException("Error when compiling expression: " + javaExpression, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} as it can copy data without evaluating the expression.
   *
   * @param tb a tuple batch
   * @param rowIdx index of the row that should be used for input data
   * @param result the table storing the result
   * @param colIdx index of the column that the result should be appended to
   * @throws InvocationTargetException exception thrown from janino
   */
  public void eval(final ReadableTable tb, final int rowIdx, final WritableColumn count, final AppendableTable result,
      final int colIdx) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    try {
      evaluator.evaluate(tb, rowIdx, count, result, colIdx);
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
   * Evaluate an expression over an entire TupleBatch and return the column of results. This method cannot take state
   * into consideration.
   *
   * @param tb the tuples to be input to this expression
   * @param result a (single-column) table containing evaluation results
   * @return a column containing the number of results from evaluating this expression on each row of {@link #tb}
   * @throws InvocationTargetException exception thrown from janino
   */
  public Column<?> evaluateColumn(final TupleBatch tb, final AppendableTable result) throws InvocationTargetException {
    ColumnBuilder<?> count = ColumnFactory.allocateColumn(Type.INT_TYPE);
    for (int rowIdx = 0; rowIdx < tb.numTuples(); ++rowIdx) {
      eval(tb, rowIdx, count, result, 0);
    }
    return count.build();
  }
}
