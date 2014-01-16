package edu.washington.escience.myria.expression;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * An Expression evaluator for generic expressions that produces a constant such as the initial state in
 * {@link StatefulApply}.
 */
public class ConstantEvaluator extends Evaluator {

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param inputSchema the schema that the expression expects if it operates on a schema
   * @param stateSchema the schema of the state
   */
  public ConstantEvaluator(final Expression expression, final Schema inputSchema, final Schema stateSchema) {
    super(expression, inputSchema, stateSchema);
  }

  /**
   * Expression evaluator.
   */
  private ExpressionEvaluator evaluator;

  /**
   * Creates an {@link ExpressionEvaluator} from the {@link #javaExpression}. This does not really compile the
   * expression and is thus faster.
   * 
   * @throws DbException compilation failed
   */
  @Override
  public void compile() throws DbException {
    try {
      evaluator =
          new ExpressionEvaluator(getExpression().getJavaExpression(), Type.OBJ_TYPE.toJavaType(), new String[] {},
              new Class[] {});
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpression()} using the {@link #evaluator}.
   * 
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Object eval() throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(new Object[] {});
  }
}
