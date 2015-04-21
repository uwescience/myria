package edu.washington.escience.myria.expression.evaluate;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.FlatteningApply;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An Expression evaluator for generic expressions. Used in {@link FlatteningApply} and {@link FlatteningStatefulApply}.
 */
public class FlatteningGenericEvaluator<T extends Type> extends Evaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * Expression evaluator.
   */
  private FlatteningExpressionEvalInterface<T> evaluator;

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
   * Helper to convert ordinary Java expressions (i.e., where isIterable() returns false) into iterable-valued
   * expressions.
   *
   * @param javaExpression Java source code string representing scalar-valued expression
   * @return Java source code string representing iterable-valued expression
   */
  private String getIterableExpressionFromScalarExpression(final String javaExpression) {
    return new StringBuilder("java.util.Collections.singletonList(").append(javaExpression).append(")").toString();
  }

  /**
   * Compiles the {@link #javaExpression}.
   *
   * @throws DbException compilation failed
   */
  @SuppressWarnings("unchecked")
  @Override
  public void compile() throws DbException {
    Preconditions.checkArgument(needsCompiling() || (getStateSchema() != null),
        "This expression does not need to be compiled.");

    // FIXME: getJavaExpressionWithAppend is a misnomer in this context
    String javaExpression = getJavaExpressionWithAppend();
    // wrap ordinary expressions in singleton iterator
    if (!getExpression().isIterable()) {
      javaExpression = getIterableExpressionFromScalarExpression(javaExpression);
    }
    IExpressionEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
    } catch (Exception e) {
      LOGGER.error("Could not create expression evaluator", e);
      throw new DbException("Could not create expression evaluator", e);
    }

    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

    try {
      evaluator =
          (FlatteningExpressionEvalInterface<T>) se
              .createFastEvaluator(javaExpression, FlatteningExpressionEvalInterface.class, new String[] {
                  Expression.TB, Expression.ROW, Expression.STATE });
    } catch (CompileException e) {
      LOGGER.error("Error when compiling expression {}: {}", javaExpression, e);
      throw new DbException("Error when compiling expression: " + javaExpression, e);
    }
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}.
   *
   * @param tb a tuple batch
   * @param rowIdx the row that should be used for input data
   * @param state additional state that affects the computation
   * @throws InvocationTargetException exception thrown from janino
   */
  public Iterable<T> eval(final ReadableTable tb, final int rowIdx, final ReadableTable state)
      throws InvocationTargetException {
    Preconditions.checkArgument(evaluator != null,
        "Call compile first or copy the data if it is the same in the input.");
    Iterable<T> it;
    try {
      it = evaluator.evaluate(tb, rowIdx, state);
    } catch (Exception e) {
      LOGGER.error(getJavaExpressionWithAppend(), e);
      throw e;
    }
    return it;
  }

}
