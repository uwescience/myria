/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

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
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonUDFEvaluator.class);
  /** python function registrar from which to fetch function pickle.*/
  private final PythonFunctionRegistrar pyFunction;

  /** python worker process. */
  private PythonWorker pyWorker;
  /** Does python function need state?*/
  private boolean needsState = false;
  /** index of state column. */
  private int stateColumnIdx = -1;
  /** tuple size to be sent to the python process, this is equal to the number of children of the expression. */
  private int tupleSize = -1;

  private int[] columnIdxs = null;
  /* Output Type of the expression. */
  private Type outputType = null;

  /** is expression a flatmap? */
  private Boolean isFlatmap = false;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   * @param pyFuncReg python function registrar to get the python function.
   */
  public PythonUDFEvaluator(
      final Expression expression,
      final ExpressionOperatorParameter parameters,
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
    isFlatmap = op.hasArrayOutputType();
    List<ExpressionOperator> childops = op.getChildren();
    tupleSize = childops.size();
    columnIdxs = new int[tupleSize];

    Arrays.fill(columnIdxs, -1);

    for (int i = 0; i < childops.size(); i++) {
      if (childops.get(i).getClass().equals(StateExpression.class)) {
        stateColumnIdx = ((StateExpression) childops.get(i)).getColumnIdx();
        columnIdxs[i] = ((StateExpression) childops.get(i)).getColumnIdx();
      } else {
        columnIdxs[i] = ((VariableExpression) childops.get(i)).getColumnIdx();
      }
    }
  }

  /**
   * Initializes the python evaluator.
   * @throws DbException
   */
  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();

    String pyFunc = ((PyUDFExpression) op).getName();
    try {

      String pyCodeString = pyFunction.getFunction(pyFunc);
      if (pyCodeString == null) {
        LOGGER.info("no python UDF with name {} registered.", pyFunc);
        throw new DbException("No Python UDf with given name registered.");
      } else {

        LOGGER.info("tuple size is: " + tupleSize);
        pyWorker.sendCodePickle(pyCodeString, tupleSize, outputType, isFlatmap);
      }

    } catch (Exception e) {
      throw new DbException(e);
    }
  }

  /**
   * Creates an {@link ExpressionEvaluator} from the {@link #javaExpression}. This does not really compile the
   * expression and is thus faster.
   */
  @Override
  public void compile() {
    /* Do nothing! */
  }

  @Override
  public void eval(
      @Nonnull final ReadableTable tb,
      final int rowIdx,
      @Nullable final WritableColumn count,
      @Nonnull final WritableColumn result,
      @Nullable final ReadableTable state)
      throws DbException, IOException {
    Object obj = evaluatePython(tb, rowIdx, state, count);
    if (obj == null) {
      throw new DbException("Python process returned null!");
    }
    try {
      switch (outputType) {
        case DOUBLE_TYPE:
          result.appendDouble((Double) obj);
          break;
        case BLOB_TYPE:
          result.appendBlob(ByteBuffer.wrap((byte[]) obj));
          break;
        case FLOAT_TYPE:
          result.appendFloat((float) obj);
          break;
        case INT_TYPE:
          result.appendInt((int) obj);
          break;
        case LONG_TYPE:
          result.appendLong((long) obj);
          break;
        default:
          LOGGER.info("Type{} not supported as python Output.", outputType.toString());
          break;
      }
    } catch (Exception e) {
      throw new DbException(e);
    }
  }

  /**
   * sendinput to be evaluated by python process.
   * @param tb -input tuples.
   * @param rowIdx -row to be evaluated.
   * @param state - state column to be updated.
   * @return Object - returned object.
   * @throws IOException
   * @throws DbException.
   */
  private Object evaluatePython(
      final ReadableTable tb,
      final int rowIdx,
      final ReadableTable state,
      final WritableColumn count)
      throws DbException, IOException {
    if (pyWorker == null) {
      pyWorker = new PythonWorker();
      initEvaluator();
    }

    try {
      if (!needsState && (stateColumnIdx != -1)) {
        throw new DbException("this evaluator should not need state!");

      } else {
        DataOutputStream dOut = pyWorker.getDataOutputStream();
        // this is replaced with number of tuples for stateful agg in the next commit.
        pyWorker.sendNumTuples(1);
        for (int i = 0; i < tupleSize; i++) {
          if (i == stateColumnIdx) {
            writeToStream(state, rowIdx, stateColumnIdx, dOut);
          } else {
            writeToStream(tb, rowIdx, columnIdxs[i], dOut);
          }
        }

        // read response back
        Object result = readFromStream(count);
        return result;
      }

    } catch (DbException e) {
      LOGGER.info("Error writing to python stream" + e.getMessage());
      throw new DbException(e);
    }
  }

  /**
   *
   * @return Object output from python process.
   * @throws DbException
   */
  private Object readFromStream(final WritableColumn count) throws DbException {
    int type = 0;
    Object obj = null;
    DataInputStream dIn = pyWorker.getDataInputStream();
    int c = 1;
    try {
      // read length of incoming message
      type = dIn.readInt();
      if (isFlatmap) {
        c = dIn.readInt();
        count.appendInt(c);
      }

      for (int i = 0; i < c; i++) {

        if (type == MyriaConstants.PYTHON_EXCEPTION) {
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));
        } else {
          // LOGGER.info("type read: " + type);
          if (type == MyriaConstants.PythonType.DOUBLE.getVal()) {
            obj = dIn.readDouble();
          } else if (type == MyriaConstants.PythonType.FLOAT.getVal()) {
            obj = dIn.readFloat();
          } else if (type == MyriaConstants.PythonType.INT.getVal()) {
            // LOGGER.info("trying to read int ");
            obj = dIn.readInt();
          } else if (type == MyriaConstants.PythonType.LONG.getVal()) {
            obj = dIn.readLong();
          } else if (type == MyriaConstants.PythonType.BYTES.getVal()) {
            int l = dIn.readInt();
            if (l > 0) {
              obj = new byte[l];
              dIn.readFully((byte[]) obj);
            }
          }
        }
      }

    } catch (Exception e) {
      LOGGER.info("Error reading from stream");
      throw new DbException(e);
    }
    return obj;
  }

  /**
   *helper function to write to python process.
   * @param tb - input tuple buffer.
   * @param row - row being evaluated.
   * @param columnIdx -columnto be written to the py process.
   * @param dOut -output stream
   * @throws DbException.
   */
  private void writeToStream(
      final ReadableTable tb, final int row, final int columnIdx, final DataOutputStream dOut)
      throws DbException {

    Preconditions.checkNotNull(tb, "input tuple cannot be null");
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
        case BLOB_TYPE:
          dOut.writeInt(MyriaConstants.PythonType.BYTES.getVal());
          // LOGGER.info("writing Bytebuffer to py process");
          ByteBuffer input = tb.getBlob(columnIdx, row);
          if (input != null && input.hasArray()) {
            // LOGGER.info("input array buffer length" + input.array().length);
            dOut.writeInt(input.array().length);
            dOut.write(input.array());
          } else {
            // LOGGER.info("input arraybuffer length is null");
            dOut.writeInt(MyriaConstants.NULL_LENGTH);
          }
      }
      dOut.flush();

    } catch (Exception e) {
      throw new DbException(e);
    }
  }
}
