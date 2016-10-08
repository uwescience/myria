/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
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
public class PythonUDFFlatteningEvaluator extends FlatteningGenericEvaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PythonUDFEvaluator.class);

  private final PythonFunctionRegistrar pyFunction;

  private final static int PYTHON_EXCEPTION = -3;
  private final static int NULL_LENGTH = -5;
  private final static int EOS = -4;

  private PythonWorker pyWorker;
  private boolean needsState = false;
  private final boolean[] isStateColumn;
  private int tupleSize = -1;
  private int[] columnIdxs = null;
  private final int isFlatmap = 1;

  private final Type outputType;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   * @param pyFuncReg python function registrar to get the python function.
   */
  public PythonUDFFlatteningEvaluator(final Expression expression, final ExpressionOperatorParameter parameters,
      final PythonFunctionRegistrar pyFuncReg) {
    super(expression, parameters);

    if (pyFuncReg != null) {
      pyFunction = pyFuncReg;
    } else {
      pyFunction = null;
    }

    if (parameters.getStateSchema() != null) {
      needsState = true;
    }

    PyUDFExpression op = (PyUDFExpression) expression.getRootExpressionOperator();
    outputType = op.getOutput();

    List<ExpressionOperator> childops = op.getChildren();
    tupleSize = childops.size();
    columnIdxs = new int[tupleSize];
    isStateColumn = new boolean[tupleSize];
    Arrays.fill(columnIdxs, -1);
    Arrays.fill(isStateColumn, false);

  }

  @Override
  public void sendEos() throws DbException {
    if (pyWorker != null) {
      pyWorker.sendEos(EOS);
    }

  }

  /**
   * Initializes the python evaluator.
   */
  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();

    String pyFunc = ((PyUDFExpression) op).getName();
    try {

      String pyCodeString = pyFunction.getUDF(pyFunc);
      if (pyCodeString == null) {
        LOGGER.info("no python UDF with name " + pyFunc);
        throw new DbException("No Python UDf with given name registered.");
      } else {
        // tuple size is
        LOGGER.info("tuple size is: " + tupleSize);
        LOGGER.info("does this eval need state? " + needsState);
        pyWorker.sendCodePickle(pyCodeString, tupleSize, outputType, isFlatmap);
      }
      List<ExpressionOperator> childops = op.getChildren();

      for (int i = 0; i < childops.size(); i++) {
        if (childops.get(i).getClass().equals(StateExpression.class)) {
          isStateColumn[i] = true;
          columnIdxs[i] = ((StateExpression) childops.get(i)).getColumnIdx();
        } else {
          columnIdxs[i] = ((VariableExpression) childops.get(i)).getColumnIdx();
        }

      }

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
    // LOGGER.info("this should be called when compiling!");
    /* Do nothing! */
  }

  /**
   * Evaluates the {@link #getJavaExpressionWithAppend()} using the {@link #evaluator}. Prefer to use
   * {@link #evaluateColumn(TupleBatch)} since it can evaluate an entire TupleBatch at a time for better locality.
   *
   * @param tb a tuple batch
   * @param rowIdx index of the row that should be used for input data
   * @param result the table storing the result
   * @param colIdx index of the column that the result should be appended to
   * @throws InvocationTargetException exception thrown from janino
   * @throws DbException
   * @throws IOException
   */
  @Override
  public void eval(final ReadableTable tb, final int rowIdx, final WritableColumn count, final AppendableTable result,
      final int colIdx) throws InvocationTargetException, DbException, IOException {

    evaluatePython(tb, rowIdx, count, result, colIdx);
  }

  private void evaluatePython(final ReadableTable tb, final int rowIdx, final WritableColumn count,
      final AppendableTable result, final int colIdx) throws DbException, IOException {
    // LOGGER.info("eval called!");
    if (pyWorker == null) {
      pyWorker = new PythonWorker();
      initEvaluator();
    }

    try {

      DataOutputStream dOut = pyWorker.getDataOutputStream();
      pyWorker.sendNumTuples(1);
      // LOGGER.info("got the output stream!");
      for (int i = 0; i < tupleSize; i++) {
        writeToStream(tb, rowIdx, columnIdxs[i], dOut);
      }

      // read response back
      readFromStream(count, result, colIdx);

    } catch (DbException e) {
      LOGGER.info("Error writing to python stream" + e.getMessage());
      throw new DbException(e);
    }
  }

  /**
   *
   * @return Object
   * @throws DbException
   */
  private void readFromStream(final WritableColumn count, final AppendableTable result, final int colIdx)
      throws DbException {

    // LOGGER.info("trying to read now");
    int type = 0;
    Object obj = null;
    DataInputStream dIn = pyWorker.getDataInputStream();

    try {

      // first read count
      int c = dIn.readInt();
      LOGGER.info("count is: " + c);
      // append count to the column
      count.appendInt(c);
      // then read the resulting tuples from stream
      for (int i = 0; i < c; i++) {
        // read length of incoming message
        type = dIn.readInt();
        if (type == PYTHON_EXCEPTION) {
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));
        } else {
          // LOGGER.info("type read: " + type);
          if (type == MyriaConstants.PythonType.DOUBLE.getVal()) {
            obj = dIn.readDouble();
            result.putDouble(colIdx, (Double) obj);
          } else if (type == MyriaConstants.PythonType.FLOAT.getVal()) {
            obj = dIn.readFloat();
            result.putFloat(colIdx, (float) obj);
          } else if (type == MyriaConstants.PythonType.INT.getVal()) {
            obj = dIn.readInt();
            result.putInt(colIdx, (int) obj);
            // LOGGER.info("int returned :" + (int) obj);
          } else if (type == MyriaConstants.PythonType.LONG.getVal()) {
            obj = dIn.readLong();
            result.putLong(colIdx, (long) obj);
          } else if (type == MyriaConstants.PythonType.BYTES.getVal()) {
            int l = dIn.readInt();
            if (l > 0) {
              // LOGGER.info("length greater than zero!");
              obj = new byte[l];
              dIn.readFully((byte[]) obj);
              result.putByteBuffer(colIdx, ByteBuffer.wrap((byte[]) obj));
            }
          }
        }
      }
      LOGGER.info("number of tuples in result: " + result.numTuples());
    } catch (Exception e) {
      LOGGER.info("Error reading from stream");
      throw new DbException(e);
    }
    return;
  }

  /**
   *
   * @param tb
   * @param row
   * @param columnIdx
   * @param dOut
   * @throws DbException
   */
  private void writeToStream(final ReadableTable tb, final int row, final int columnIdx, final DataOutputStream dOut)
      throws DbException {

    Preconditions.checkNotNull(tb, "tuple input cannot be null");
    Preconditions.checkNotNull(dOut, "Output stream for python process cannot be null");

    Schema tbsc = tb.getSchema();
    // LOGGER.info("tuple batch schema " + tbsc.toString());
    try {
      Type type = tbsc.getColumnType(columnIdx);
      // LOGGER.info("column index " + columnIdx + " columnType " + type.toString());
      switch (type) {
        case BOOLEAN_TYPE:
          LOGGER.info("BOOLEAN type not supported for python function ");
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
          // LOGGER.info("writing int to py process");
          break;
        case LONG_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.LONG.getVal());
          dOut.writeInt(Long.SIZE / Byte.SIZE);
          dOut.writeLong(tb.getLong(columnIdx, row));
          break;
        case STRING_TYPE:
          LOGGER.info("STRING type is not yet supported for python function ");
          break;
        case DATETIME_TYPE:
          LOGGER.info("date time not yet supported for python function ");
          break;
        case BYTES_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.BYTES.getVal());
          // LOGGER.info("writing Bytebuffer to py process");
          ByteBuffer input = tb.getByteBuffer(columnIdx, row);
          if (input != null && input.hasArray()) {
            // LOGGER.info("input array buffer length" + input.array().length);
            dOut.writeInt(input.array().length);
            dOut.write(input.array());
          } else {
            // LOGGER.info("input arraybuffer length is null");
            dOut.writeInt(NULL_LENGTH);
          }
      }
      // LOGGER.info("flushing data");
      dOut.flush();

    } catch (Exception e) {
      // LOGGER.info(e.getMessage());
      throw new DbException(e);
    }
  }
}
