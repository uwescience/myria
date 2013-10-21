package edu.washington.escience.myria.expression;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.column.Column;

/**
 * An expression that can be applied to a tuple. This is the generic version where the evaluator returns objects.
 */
public class ObjectExpression extends Expression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Expression evaluator.
   */
  private Evaluator evaluator;

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public ObjectExpression(final String outputName, final ExpressionOperator rootExpressionOperator) {
    super(outputName, rootExpressionOperator);
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  public void compile() throws DbException {
    Preconditions.checkArgument(!isCopyFromInput(),
        "This expression does not need to be compiled because the data can be copied from the input.");
    setJavaExpression(getRootExpressionOperator().getJavaString(Objects.requireNonNull(getInputSchema())));

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator =
          (Evaluator) se.createFastEvaluator(getJavaExpression(), Evaluator.class, new String[] { "tb", "rowId" });
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the expression using the {@link #evaluator}. Prefer to use
   * {@link #evalAndPut(TupleBatch, int, TupleBatchBuffer, int)} as it can copy data without evaluating the expression.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(tb, rowId);
  }

  @Override
  public void setSchema(final Schema inputSchema) {
    evaluator = null;
    super.setSchema(inputSchema);
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
   * @throws InvocationTargetException exception thrown from janino
   */
  @SuppressWarnings("deprecation")
  public void evalAndPut(final TupleBatch sourceTupleBatch, final int sourceRowIdx,
      final TupleBatchBuffer targetTupleBuffer, final int targetColumnIdx) throws InvocationTargetException {
    if (isCopyFromInput()) {
      final Column<?> sourceColumn =
          sourceTupleBatch.getDataColumns().get(((VariableExpression) getRootExpressionOperator()).getColumnIdx());
      targetTupleBuffer.put(targetColumnIdx, sourceColumn, sourceRowIdx);
    } else {
      Object result = eval(sourceTupleBatch, sourceRowIdx);
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      targetTupleBuffer.put(targetColumnIdx, result);
    }

  }
}
