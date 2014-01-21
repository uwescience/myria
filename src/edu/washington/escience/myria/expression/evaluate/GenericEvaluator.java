package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.ReadableTable;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * An Expression evaluator for generic expressions. Used in {@link Apply} and {@link StatefulApply}.
 */
public class GenericEvaluator extends TupleEvaluator {

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param inputSchema the schema that the expression expects
   * @param stateSchema the schema of the state
   */
  public GenericEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
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
  public Object eval(final TupleBatch tb, final int rowId, final ReadableTable state) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(tb, rowId, state);
  }

  /**
   * Evaluate an expression over an entire TupleBatch and return the column of results. This method cannot take state
   * into consideration.
   * 
   * @param tb the tuples to be input to this expression
   * @return a column containing the result of evaluating this expression on the entire TupleBatch
   * @throws InvocationTargetException exception thrown from janino
   */
  @SuppressWarnings("deprecation")
  public Column<?> evaluateColumn(final TupleBatch tb) throws InvocationTargetException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    /* This expression just copies an input column. */
    if (isCopyFromInput()) {
      return tb.getDataColumns().get(((VariableExpression) op).getColumnIdx());
    }

    Type type = getOutputType();

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
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      ret.appendObject(eval(tb, row, null));
    }
    return ret.build();
  }
}
