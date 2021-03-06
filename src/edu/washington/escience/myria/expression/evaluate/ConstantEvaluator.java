package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * An Expression evaluator for generic expressions that produces a constant such as the initial state in
 * {@link StatefulApply}.
 */
public final class ConstantEvaluator extends GenericEvaluator {
  /** An empty object array passed to Janino as no arguments. */
  private static final Object[] NO_ARGS = new Object[] {};
  /** The value of this expression. */
  private final Object value;
  /** The type of the value of this expression. */
  private final Type type;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   * @throws DbException if there is an error compiling the expression
   */
  public ConstantEvaluator(
      final Expression expression, final ExpressionOperatorParameter parameters)
      throws DbException {
    super(expression, parameters);
    Preconditions.checkArgument(
        expression.isConstant(), "Expression %s does not evaluate to a constant", expression);
    Preconditions.checkArgument(
        !expression.isMultiValued(), "Expression %s is multivalued", expression);
    type = expression.getOutputType(parameters);
    String java;
    try {
      java = expression.getJavaExpression(parameters);
    } catch (Exception e) {
      throw new DbException("Error when generating Java expression " + this, e);
    }

    evaluator = new ExpressionEvaluator();
    evaluator.setParameters(new String[] {}, new Class<?>[] {});
    evaluator.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

    try {
      evaluator.setExpressionType(type.toJavaType());
      evaluator.cook(java);
      value = evaluator.evaluate(NO_ARGS);
    } catch (CompileException e) {
      throw new DbException("Error when compiling expression " + java, e);
    } catch (InvocationTargetException e) {
      throw new DbException("Error when evaluating expression " + java, e);
    }
  }

  /**
   * Expression evaluator.
   */
  private final ExpressionEvaluator evaluator;

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}.
   *
   * @return the result from the evaluation
   */
  public Object eval() {
    return value;
  }

  @Override
  public void eval(
      final ReadableTable input,
      final int inputRow,
      final ReadableTable state,
      final int stateRow,
      final WritableColumn result,
      final WritableColumn count) {
    Preconditions.checkArgument(
        count == null, "Count column must be null for constant expressions");
    result.appendObject(value);
  }

  @Override
  public EvaluatorResult evalTupleBatch(final TupleBatch tb, final Schema outputSchema)
      throws DbException {
    if (TupleUtils.getBatchSize(outputSchema) == tb.getBatchSize()) {
      return new EvaluatorResult(
          new ConstantValueColumn((Comparable<?>) value, type, tb.numTuples()),
          new ConstantValueColumn(1, Type.INT_TYPE, tb.numTuples()));
    }
    return super.evalTupleBatch(tb, outputSchema);
  }
}
