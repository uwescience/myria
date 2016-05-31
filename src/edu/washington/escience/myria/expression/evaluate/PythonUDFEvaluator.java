/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import java.nio.ByteBuffer;

import org.codehaus.janino.ExpressionEvaluator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.PyUDFExpression;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An Expression evaluator for Python UDFs. Used in {@link Apply} and {@link StatefulApply}.
 */

public class PythonUDFEvaluator extends GenericEvaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);
  private final PythonFunctionRegistrar pyFunction;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   */
  public PythonUDFEvaluator(final Expression expression, final ExpressionOperatorParameter parameters,
      final PythonFunctionRegistrar pyFuncReg) {
    super(expression, parameters);
    // parameters and expression are saved in the super
    LOGGER.info(getExpression().getOutputName());
    pyFunction = pyFuncReg;
    try {
      initEvaluator();
    } catch (DbException e) {
      // TODO Auto-generated catch block
      LOGGER.info(e.getMessage());
    }

  }

  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    String pyFunc = ((PyUDFExpression) op).getName();
    LOGGER.info(pyFunc);
    // pyFunction = new RegisterFunction(null);
    // get master catalog
    // retrieve pickled function
    try {
      ByteBuffer pyPickle = pyFunction.getUDF(pyFunc);
      LOGGER.info("Got code pickle");
      int i = pyPickle.array().length;
      LOGGER.info("pickle length" + i);
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      throw new DbException(e);
    }
    // start python worker

  }

  /**
   * Creates an {@link ExpressionEvaluator} from the {@link #javaExpression}. This does not really compile the
   * expression and is thus faster.
   */
  @Override
  public void compile() {
    LOGGER.info("this should be called when compiling!");
    /* Do nothing! */
  }

  @Override
  public void eval(final ReadableTable tb, final int rowIdx, final WritableColumn result, final ReadableTable state) {

    try {
      LOGGER.info("call evaluate on the evaluator!");
      LOGGER.info("this is where the pickle should be sent to the py process along with the tuple");
      // evaluator.evaluate(tb, rowIdx, result, state);
    } catch (Exception e) {
      LOGGER.error(getJavaExpressionWithAppend(), e);
      throw e;
    }
  }

  @Override
  public Column<?> evaluateColumn(final TupleBatch tb) {
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    LOGGER.info("trying to evaluate column for tuple batch");
    Type type = getOutputType();

    ColumnBuilder<?> ret = ColumnFactory.allocateColumn(type);
    for (int row = 0; row < tb.numTuples(); ++row) {
      /** We already have an object, so we're not using the wrong version of put. Remove the warning. */
      LOGGER.info("for every row in the tuple batch eval row in the tuple batch and stick the return value in ret! ");
      eval(tb, row, ret, null);
    }
    return ret.build();
  }
}
