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
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;

/**
 * An expression that can be applied to a tuple. This is the generic version where the evaluator returns objects.
 */
public class GenericExpression extends Expression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Expression evaluator.
   */
  private Evaluator evaluator;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private GenericExpression() {
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public GenericExpression(final String outputName, final ExpressionOperator rootExpressionOperator) {
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
   * Evaluates the {@link #getJavaExpression()} using the {@link #evaluator}. Prefer to use
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

  /**
   * Exaluate an expression over an entire TupleBatch and return the column of results.
   * 
   * @param tb the tuples to be input to this expression
   * @return a column containing the result of evaluating this expression on the entire TupleBatch
   * @throws InvocationTargetException exception thrown from janino
   */
  public Column<?> evaluateColumn(final TupleBatch tb) throws InvocationTargetException {
    ExpressionOperator op = getRootExpressionOperator();
    /* This expression just copies an input column. */
    if (op instanceof VariableExpression) {
      return tb.getDataColumns().get(((VariableExpression) op).getColumnIdx());
    }

    Type type = op.getOutputType(getInputSchema());

    /* This expression is a constant. */
    if (op instanceof ConstantExpression) {
      ConstantExpression constOp = (ConstantExpression) op;
      return new ConstantValueColumn(type.fromString(constOp.getValue()), type, tb.numTuples());
    }
    /*
     * TODO for efficiency handle expressions that evaluate to a constant, e.g., they don't contain any
     * VariableExpressions.
     */

    ColumnBuilder<?> ret = ColumnFactory.allocateColumn(type);
    for (int row = 0; row < tb.numTuples(); ++row) {
      ret.appendObject(eval(tb, row));
    }
    return ret.build();
  }
}