/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.PyUDFExpression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.functions.PythonWorker;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An Expression evaluator for Python UDFs. Used in {@link Apply} and {@link StatefulApply}.
 */

public class PythonUDFEvaluator extends GenericEvaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PythonUDFEvaluator.class);
  private final PythonFunctionRegistrar pyFunction;

  private final static int PYTHON_EXCEPTION = -3;
  private final static int NULL_LENGTH = -5;

  private PythonWorker pyWorker;
  private boolean needsState = false;
  private int leftColumnIdx = -1;
  private int rightColumnIdx = -1;

  private boolean bLeftState = false;
  private boolean bRightState = false;

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
    LOGGER.info("Output name for the python expression" + getExpression().getOutputName());
    if (pyFuncReg != null) {
      pyFunction = pyFuncReg;
    } else {
      pyFunction = null;
    }
    if (parameters.getStateSchema() != null) {
      needsState = true;
    }
    PyUDFExpression op = (PyUDFExpression) expression.getRootExpressionOperator();

    ExpressionOperator left = op.getLeft();
    LOGGER.info("left string :" + left.toString());

    ExpressionOperator right = op.getRight();
    LOGGER.info("right string :" + right.toString());

    if (left.getClass().equals(VariableExpression.class)) {
      leftColumnIdx = ((VariableExpression) left).getColumnIdx();

    } else if (left.getClass().equals(StateExpression.class)) {
      leftColumnIdx = ((StateExpression) left).getColumnIdx();
      bLeftState = true;
    }

    if (right.getClass().equals(VariableExpression.class)) {
      rightColumnIdx = ((VariableExpression) right).getColumnIdx();

    } else if (right.getClass().equals(StateExpression.class)) {
      rightColumnIdx = ((StateExpression) right).getColumnIdx();
      bRightState = true;
    }

    LOGGER.info("Left column ID " + leftColumnIdx + "left variable? " + bLeftState);
    LOGGER.info("right column ID " + rightColumnIdx + "right variable? " + bRightState);

  }

  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();

    String pyFunc = ((PyUDFExpression) op).getName();
    LOGGER.info(pyFunc);

    try {
      String pyCodeString = pyFunction.getUDF(pyFunc);
      LOGGER.info("length of string from postgres " + pyCodeString.length());
      pyWorker.sendCodePickle(pyCodeString, 2);// tuple size is always 2 for binary expression.

    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      throw new DbException(e);
    }

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
  public void eval(final ReadableTable tb, final int rowIdx, final WritableColumn result, final ReadableTable state)
      throws DbException {
    Object obj = evaluatePython(tb, rowIdx, state);
    result.appendByteBuffer(ByteBuffer.wrap((byte[]) obj));
  }

  public void evalUpdatePyExpression(final ReadableTable tb, final int rowIdx, final AppendableTable result,
      final ReadableTable state) throws DbException {
    // this is only called when updating the state tuple
    Object obj = evaluatePython(tb, rowIdx, state);
    int resultcol = -1;
    if (bLeftState) {
      resultcol = leftColumnIdx;
    }
    if (bRightState) {
      resultcol = rightColumnIdx;
    }
    LOGGER.info("trying to update state on column: " + resultcol);

    result.putByteBuffer(resultcol, (ByteBuffer.wrap((byte[]) obj)));

  }

  private Object evaluatePython(final ReadableTable tb, final int rowIdx, final ReadableTable state) throws DbException {
    LOGGER.info("eval called!");
    if (pyWorker == null) {
      pyWorker = new PythonWorker();
      initEvaluator();
    }

    try {
      if (needsState == false && (bRightState || bLeftState)) {
        LOGGER.info("needs State: " + needsState + " Right column is state: " + bRightState + " Left column is state: "
            + bLeftState);
        throw new DbException("this evaluator should not need state!");

      } else {
        DataOutputStream dOut = pyWorker.getDataOutputStream();
        LOGGER.info("got the output stream!");
        ReadableTable readbuffer;
        // send left column
        if (bLeftState) {
          readbuffer = state;
        } else {
          readbuffer = tb;
        }
        writeToStream(readbuffer, rowIdx, leftColumnIdx, dOut);
        // send right column
        if (bRightState) {
          readbuffer = state;
        } else {
          readbuffer = tb;
        }
        writeToStream(readbuffer, rowIdx, rightColumnIdx, dOut);
        // read response back
        Object result = readFromStream();
        return result;
      }

    } catch (DbException e) {
      LOGGER.info("Error writing to python stream" + e.getMessage());
      throw new DbException(e);
    }

  }

  @Override
  public Column<?> evaluateColumn(final TupleBatch tb) throws DbException {

    Type type = getOutputType();
    ColumnBuilder<?> ret = ColumnFactory.allocateColumn(type);
    for (int row = 0; row < tb.numTuples(); ++row) {
      eval(tb, row, ret, null);
    }
    return ret.build();
  }

  private Object readFromStream() throws DbException {
    LOGGER.info("trying to read now");
    int length = 0;
    byte[] obj = null;
    DataInputStream dIn = pyWorker.getDataInputStream();

    try {
      length = dIn.readInt(); // read length of incoming message
      switch (length) {
        case PYTHON_EXCEPTION:
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));

        default:
          if (length > 0) {
            LOGGER.info("length greater than zero!");
            obj = new byte[length];
            dIn.readFully(obj);
          }
          break;
      }

    } catch (Exception e) {
      LOGGER.info("Error reading int from stream");
      throw new DbException(e);
    }
    return obj;

  }

  private void writeToStream(final ReadableTable tb, final int row, final int columnIdx, final DataOutputStream dOut)
      throws DbException {

    Preconditions.checkNotNull(tb, "tuple input cannot be null");
    Preconditions.checkNotNull(dOut, "Output stream for python process cannot be null");

    Schema tbsc = tb.getSchema();
    LOGGER.info("tuple batch schema " + tbsc.toString());
    try {
      Type type = tbsc.getColumnType(columnIdx);
      LOGGER.info("column index " + columnIdx + " columnType " + type.toString());
      switch (type) {
        case BOOLEAN_TYPE:
          LOGGER.info("BOOLEAN type not supported for python UDF ");
          break;
        case DOUBLE_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.DOUBLE.getVal());
          dOut.writeInt(Double.SIZE / Byte.SIZE);
          dOut.writeDouble(tb.getDouble(columnIdx, row));
          break;
        case FLOAT_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.FLOAT.getVal());
          dOut.writeInt(Float.SIZE / Byte.SIZE);
          dOut.writeFloat(tb.getFloat(columnIdx, row));
          break;
        case INT_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.INT.getVal());
          dOut.writeInt(Integer.SIZE / Byte.SIZE);
          dOut.writeInt(tb.getInt(columnIdx, row));
          LOGGER.info("writing int to py process");
          break;
        case LONG_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.LONG.getVal());
          dOut.writeInt(Long.SIZE / Byte.SIZE);
          dOut.writeLong(tb.getLong(columnIdx, row));
          break;
        case STRING_TYPE:
          LOGGER.info("STRING type is not yet supported for python UDF ");
          break;
        case DATETIME_TYPE:
          LOGGER.info("date time not yet supported for python UDF ");
          break;
        case BYTES_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.BYTES.getVal());
          LOGGER.info("writing Bytebuffer to py process");
          ByteBuffer input = tb.getByteBuffer(columnIdx, row);
          if (input != null && input.hasArray()) {
            LOGGER.info("input array buffer length" + input.array().length);
            dOut.writeInt(input.array().length);
            dOut.write(input.array());
          } else {
            LOGGER.info("input arraybuffer length is null");
            dOut.writeInt(NULL_LENGTH);
          }
      }
      LOGGER.info("flushing data");
      dOut.flush();

    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      throw new DbException(e);
    }

  }

  /**
   * @param from
   * @param row
   * @param stateTuple
   * @param stateTuple2
   */

}
