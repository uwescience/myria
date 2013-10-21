/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;

/**
 * An expression that can be applied to a tuple. This is the specialized version where the evaluator returns booleans.
 */
public class BooleanExpression extends Expression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Expression evaluator.
   */
  private BooleanEvaluator evaluator;

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public BooleanExpression(final String outputName, final ExpressionOperator rootExpressionOperator) {
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
    Preconditions.checkArgument(getRootExpressionOperator().getOutputType(getInputSchema()) == Type.BOOLEAN_TYPE);
    setJavaExpression(getRootExpressionOperator().getJavaString(Objects.requireNonNull(getInputSchema())));

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

      evaluator =
          (BooleanEvaluator) se.createFastEvaluator(getJavaExpression(), BooleanEvaluator.class, new String[] {
              "tb", "rowId" });
    } catch (Exception e) {
      throw new DbException("Error when compiling expression " + this, e);
    }
  }

  /**
   * Evaluates the expression using the {@link #evaluator}.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   * @throws InvocationTargetException exception thrown from janino
   */
  public boolean eval(final TupleBatch tb, final int rowId) throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    return evaluator.evaluate(tb, rowId);
  }

  @Override
  public void setSchema(final Schema inputSchema) {
    evaluator = null;
    super.setSchema(inputSchema);
  }
}
