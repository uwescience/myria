/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

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
import edu.washington.escience.myria.api.encoding.FunctionStatus;
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
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An Expression evaluator for Python UDFs. Used in {@link Apply} and {@link StatefulApply}.
 */
public class PythonUDFEvaluator extends GenericEvaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonUDFEvaluator.class);
  /** python function registrar from which to fetch function pickle.*/
  private final PythonFunctionRegistrar pyFuncRegistrar;

  /** python worker process. */
  private PythonWorker pyWorker;
  /** index of state column. */
  private final boolean[] isStateColumn;
  /** tuple size to be sent to the python process, this is equal to the number of children of the expression. */
  private int numColumns = -1;

  private int[] columnIdxs = null;

  /** Output Type of the expression. */
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
      @NotNull final PythonFunctionRegistrar pyFuncReg) {
    super(expression, parameters);
    pyFuncRegistrar = pyFuncReg;

    PyUDFExpression op = (PyUDFExpression) expression.getRootExpressionOperator();
    outputType = op.getOutput();
    List<ExpressionOperator> childops = op.getChildren();
    numColumns = childops.size();
    columnIdxs = new int[numColumns];
    isStateColumn = new boolean[numColumns];

    Arrays.fill(isStateColumn, false);
    Arrays.fill(columnIdxs, -1);
  }

  /**
   * Initializes the python evaluator.
   * @throws DbException
   */
  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();

    String pyFunctionName = ((PyUDFExpression) op).getName();
    LOGGER.info("trying to initialize evaluator");

    try {
      if (pyFuncRegistrar != null) {
        LOGGER.info("py func registrar is not null");
        LOGGER.info("py function name: " + pyFunctionName);
        FunctionStatus fs = pyFuncRegistrar.getFunctionStatus(pyFunctionName);

        if (fs != null && fs.getName() == null) {

          LOGGER.info("no python UDF with name {} registered.", pyFunctionName);
          throw new DbException("No Python UDf with given name registered.");
        } else {
          // set output type
          LOGGER.info(fs.getOutputType() + fs.getName() + fs.getLanguage());
          if (pyWorker != null) {
            pyWorker.sendCodePickle(fs.getBinary(), numColumns, outputType, fs.getIsMultivalued());
          }
        }
      }
      List<ExpressionOperator> childops = op.getChildren();
      if (childops != null) {

        for (int i = 0; i < childops.size(); i++) {

          if (childops.get(i).getClass().equals(StateExpression.class)) {
            isStateColumn[i] = true;
            columnIdxs[i] = ((StateExpression) childops.get(i)).getColumnIdx();

          } else if (childops.get(i).getClass().equals(VariableExpression.class)) {
            columnIdxs[i] = ((VariableExpression) childops.get(i)).getColumnIdx();
          } else {
            throw new DbException(
                "Python expression can only have State or Variable expression as child expressions.");
          }
        }
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
   *
   * @param ltb list of tuple batch
   * @param result result table.
   * @param state state column.
   * @throws DbException in case of error
   * @throws IOException in case of error.
   */
  public void evalBatch(
      final List<TupleBatch> ltb, final AppendableTable result, final ReadableTable state)
      throws DbException, IOException {
    if (pyWorker == null) {
      pyWorker = new PythonWorker();
      initEvaluator();
    }
    int resultcol = -1;
    for (int i = 0; i < numColumns; i++) {
      if (isStateColumn[i]) {
        resultcol = columnIdxs[i];
      }
      break;
    }
    try {

      DataOutputStream dOut = pyWorker.getDataOutputStream();
      int numTuples = 0;
      for (int j = 0; j < ltb.size(); j++) {
        numTuples += ltb.get(j).numTuples();
      }
      pyWorker.sendNumTuples(numTuples);

      for (int tbIdx = 0; tbIdx < ltb.size(); tbIdx++) {
        TupleBatch tb = ltb.get(tbIdx);
        for (int tup = 0; tup < tb.numTuples(); tup++) {
          for (int col = 0; col < numColumns; col++) {
            writeToStream(tb, tup, columnIdxs[col], dOut);
          }
        }
      }

      // read result back
      Object obj = readFromStream(null);
      switch (outputType) {
        case DOUBLE_TYPE:
          result.putDouble(resultcol, (Double) obj);
          break;
        case BLOB_TYPE:
          result.putBlob(resultcol, (ByteBuffer.wrap((byte[]) obj)));
          break;
        case FLOAT_TYPE:
          result.putFloat(resultcol, (float) obj);
          break;
        case INT_TYPE:
          result.putInt(resultcol, (int) obj);
          break;
        case LONG_TYPE:
          result.putLong(resultcol, (long) obj);
          break;

        default:
          LOGGER.info("type not supported as Python Output");
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
      DataOutputStream dOut = pyWorker.getDataOutputStream();
      pyWorker.sendNumTuples(1);
      for (int i = 0; i < numColumns; i++) {
        if (isStateColumn[i]) {
          writeToStream(state, rowIdx, columnIdxs[i], dOut);
        } else {
          writeToStream(tb, rowIdx, columnIdxs[i], dOut);
        }
      }

      // read response back
      Object result = readFromStream(count);
      return result;

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
    int c = 1; // single valued expressions only return 1 tuple.
    try {
      // if it is a flat map operation, read number of tuples to be read.
      if (isFlatmap) {
        c = dIn.readInt();
        count.appendInt(c);
      }

      for (int i = 0; i < c; i++) {
        //then read the type of tuple
        type = dIn.readInt();
        // if the 'type' is exception, throw exception
        if (type == MyriaConstants.PythonSpecialLengths.PYTHON_EXCEPTION.getVal()) {
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));
        } else {
          // read the rest of the tuple
          if (type == MyriaConstants.PythonType.DOUBLE.getVal()) {
            obj = dIn.readDouble();
          } else if (type == MyriaConstants.PythonType.FLOAT.getVal()) {
            obj = dIn.readFloat();
          } else if (type == MyriaConstants.PythonType.INT.getVal()) {

            obj = dIn.readInt();
          } else if (type == MyriaConstants.PythonType.LONG.getVal()) {
            obj = dIn.readLong();
          } else if (type == MyriaConstants.PythonType.BLOB.getVal()) {
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

    try {
      Type type = tbsc.getColumnType(columnIdx);

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
          dOut.writeInt(MyriaConstants.PythonType.BLOB.getVal());

          ByteBuffer input = tb.getBlob(columnIdx, row);

          if (input != null && input.hasArray()) {

            dOut.writeInt(input.array().length);
            dOut.write(input.array());
          } else {

            dOut.writeInt(MyriaConstants.PythonSpecialLengths.NULL_LENGTH.getVal());
          }
      }
      dOut.flush();

    } catch (Exception e) {
      throw new DbException(e);
    }
  }

  @Override
  public void sendEos() throws DbException {
    LOGGER.info("sendEOS called");
    if (pyWorker != null) {
      pyWorker.sendEos(MyriaConstants.PythonSpecialLengths.EOS.getVal());
    }
  }
}
