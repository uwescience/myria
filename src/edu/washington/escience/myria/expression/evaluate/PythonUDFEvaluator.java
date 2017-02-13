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
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
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
import edu.washington.escience.myria.profiling.ProfilingLogger;
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
  private Boolean isMultiValued = false;

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
    outputType = op.getOutputType(parameters);
    List<ExpressionOperator> childops = op.getChildren();
    numColumns = childops.size();
    columnIdxs = new int[numColumns];
    isStateColumn = new boolean[numColumns];

    Arrays.fill(isStateColumn, false);
    Arrays.fill(columnIdxs, -1);
  }

  /**
   * Initializes the python evaluator.
   * @throws DbException in case of error.
   */
  private void initEvaluator() throws DbException {
    ExpressionOperator op = getExpression().getRootExpressionOperator();
    String pyFunctionName = ((PyUDFExpression) op).getName();

    try {
      if (pyFuncRegistrar != null) {
        FunctionStatus fs = pyFuncRegistrar.getFunctionStatus(pyFunctionName);
        if (fs == null) {
          throw new DbException("No Python UDf with given name registered.");
        }
        isMultiValued = fs.getIsMultivalued(); //if the function is multivalued.
        pyWorker.sendCodePickle(fs.getBinary(), numColumns, outputType, isMultiValued);

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
      } else {
        throw new DbException("PythonRegistrar should not be null in  PythonUDFEvaluator.");
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
      throws DbException {

    if (pyWorker == null) {
      pyWorker = new PythonWorker();
      initEvaluator();
    }
    int resultColIdx = -1;

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
      readFromStream(count, result, null, resultColIdx);

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
      readFromStream(null, null, result, resultcol);

    } catch (Exception e) {
      throw new DbException(e);
    }
  }

  /**
   *@param count number of tuples returned.
   *@param result writable column
   *@param result2 appendable table
   *@param resultColIdx id of the result column.
   * @return Object output from python process.
   * @throws DbException in case of error.
   */
  private Object readFromStream(
      final WritableColumn count,
      final WritableColumn result,
      final AppendableTable result2,
      final int resultColIdx)
      throws DbException {
    int type = 0;
    Object obj = null;
    DataInputStream dIn = pyWorker.getDataInputStream();

    int c = 1; // single valued expressions only return 1 tuple.
    try {
      // if it is a flat map operation, read number of tuples to be read.
      if (isMultiValued) {
        c = dIn.readInt();
        count.appendInt(c);
      } else { // not flatmap
        if (count != null) { // count column is not null
          count.appendInt(1);
        }
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
            if (resultColIdx == -1) {
              result.appendDouble((Double) obj);
            } else {
              result2.putDouble(resultColIdx, (Double) obj);
            }
          } else if (type == MyriaConstants.PythonType.FLOAT.getVal()) {
            obj = dIn.readFloat();
            if (resultColIdx == -1) {
              result.appendFloat((float) obj);
            } else {
              result2.putFloat(resultColIdx, (float) obj);
            }

          } else if (type == MyriaConstants.PythonType.INT.getVal()) {
            obj = dIn.readInt();
            if (resultColIdx == -1) {
              result.appendInt((int) obj);
            } else {
              result2.putInt(resultColIdx, (int) obj);
            }

          } else if (type == MyriaConstants.PythonType.LONG.getVal()) {
            obj = dIn.readLong();
            if (resultColIdx == -1) {
              result.appendLong((long) obj);
            } else {
              result2.putLong(resultColIdx, (long) obj);
            }

          } else if (type == MyriaConstants.PythonType.BLOB.getVal()) {

            int l = dIn.readInt();
            if (l > 0) {
              obj = new byte[l];
              dIn.readFully((byte[]) obj);
              if (resultColIdx == -1) {
                result.appendBlob(ByteBuffer.wrap((byte[]) obj));
              } else {
                result2.putBlob(resultColIdx, ByteBuffer.wrap((byte[]) obj));
              }
            }

          } else {
            throw new DbException("Type not supported by python");
          }
        }
      }

    } catch (Exception e) {
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
   * @throws DbException in case of error.
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
          LOGGER.debug("BOOLEAN type not supported for python function ");
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
          LOGGER.debug("STRING type is not yet supported for python function ");
          break;
        case DATETIME_TYPE:
          LOGGER.debug("date time not yet supported for python function ");
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
}
