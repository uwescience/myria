package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.ReadableTable;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ConstantValueColumn;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.StatefulApply;

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
   * @param inputSchema the schema that the expression expects if it operates on a schema
   * @param stateSchema the schema of the state
   * @throws DbException if there is an error compiling the expression
   */
  public ConstantEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema)
      throws DbException {
    super(expression, inputSchema, stateSchema);
    Preconditions.checkArgument(!expression.hasOperator(VariableExpression.class)
        && !expression.hasOperator(StateExpression.class), "Expression %s does not evaluate to a constant", expression);
    type = expression.getOutputType(inputSchema, stateSchema);
    try {
      evaluator =
          new ExpressionEvaluator(expression.getJavaExpression(), type.toJavaType(), new String[] {}, new Class<?>[] {});
      value = evaluator.evaluate(NO_ARGS);
    } catch (CompileException e) {
      throw new DbException("Error when compiling expression " + this, e);
    } catch (InvocationTargetException e) {
      throw new DbException("Error when evaluating expression " + this, e);
    }
  }

  /**
   * Expression evaluator.
   */
  private ExpressionEvaluator evaluator;

  /**
   * Creates an {@link ExpressionEvaluator} from the {@link #javaExpression}. This does not really compile the
   * expression and is thus faster.
   */
  @Override
  public void compile() {
    /* Do nothing! */
  }

  /**
   * Evaluates the {@link #getJavaExpression()} using the {@link #evaluator}.
   * 
   * @return the result from the evaluation
   */
  public Object eval() {
    return value;
  }

  @Override
  public void eval(final TupleBatch tb, final int rowIdx, final WritableColumn result, final ReadableTable state) {
    throw new UnsupportedOperationException("Should not be here. Should be using eval() instead");
  }

  @Override
  public Column<?> evaluateColumn(final TupleBatch tb) {
    return new ConstantValueColumn((Comparable<?>) value, type, tb.numTuples());
  }
}
