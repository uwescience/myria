package edu.washington.escience.myria.expression;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.Apply;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply}.
 */
public class GenericEvaluator extends TupleEvaluator {

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param schema the schema that the expression expects
   */
  public GenericEvaluator(final Expression expression, final Schema schema) {
    super(expression, schema);
  }

  /**
   * Expression evaluator.
   */
  private EvalInterface evaluator;

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    Preconditions.checkArgument(!isCopyFromInput(),
        "This expression does not need to be compiled because the data can be copied from the input.");

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator =
          (EvalInterface) se.createFastEvaluator(getJavaExpression(), EvalInterface.class, new String[] {
              "tb", "rowId", "state" });
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpression()} using the {@link #evaluator}. Prefer to use
   * {@link #evalAndPut(TupleBatch, int, TupleBatchBuffer, int)} as it can copy data without evaluating the expression.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @param state additional state that affects the computation
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval(final TupleBatch tb, final int rowId, final Object state) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(tb, rowId, state);
  }

  /**
   * Runs {@link #eval(TupleBatch, int)} if necessary and puts the result in the target tuple buffer.
   * 
   * If evaluating is not necessary, the data is copied directly from the source tuple batch into the target buffer.
   * 
   * @param sourceTupleBatch the tuple buffer that should be used as input
   * @param sourceRowIdx the row that should be used in the input batch
   * @param targetTupleBuffer the tuple buffer that should be used as output
   * @param targetColumnIdx the column that the data should be written to
   * @param state
   * @throws InvocationTargetException exception thrown from janino
   */
  public void evalAndPut(final TupleBatch sourceTupleBatch, final int sourceRowIdx,
      final TupleBatchBuffer targetTupleBuffer, final int targetColumnIdx) throws InvocationTargetException {
    evalAndPut(sourceTupleBatch, sourceRowIdx, targetTupleBuffer, targetColumnIdx, null);
  }

  /**
   * Runs {@link #eval(TupleBatch, int)} if necessary and puts the result in the target tuple buffer.
   * 
   * If evaluating is not necessary, the data is copied directly from the source tuple batch into the target buffer.
   * 
   * @param sourceTupleBatch the tuple buffer that should be used as input
   * @param sourceRowIdx the row that should be used in the input batch
   * @param targetTupleBuffer the tuple buffer that should be used as output
   * @param targetColumnIdx the column that the data should be written to
   * @param state additional state that affects the result
   * @throws InvocationTargetException exception thrown from janino
   */
  @SuppressWarnings("deprecation")
  public void evalAndPut(final TupleBatch sourceTupleBatch, final int sourceRowIdx,
      final TupleBatchBuffer targetTupleBuffer, final int targetColumnIdx, final Object state)
      throws InvocationTargetException {
    if (isCopyFromInput()) {
      final Column<?> sourceColumn =
          sourceTupleBatch.getDataColumns().get(
              ((VariableExpression) getExpression().getRootExpressionOperator()).getColumnIdx());
      targetTupleBuffer.put(targetColumnIdx, sourceColumn, sourceRowIdx);
    } else {
      Object result = eval(sourceTupleBatch, sourceRowIdx, state);
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      targetTupleBuffer.put(targetColumnIdx, result);
    }

  }
}
