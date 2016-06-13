/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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
import edu.washington.escience.myria.functions.PythonWorker;
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
  private PythonWorker pyWorker;

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

      pyWorker = new PythonWorker();
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
  public void eval(final ReadableTable tb, final int rowIdx, final WritableColumn result, final ReadableTable state) {

    // try {
    // evaluator.evaluate(tb, rowIdx, result, state);

  }

  @Override
  public Column<?> evaluateColumn(final TupleBatch tb) throws DbException {

    LOGGER.info("trying to evaluate column for tuple batch");
    Type type = getOutputType();

    ColumnBuilder<?> ret = ColumnFactory.allocateColumn(type);
    for (int row = 0; row < tb.numTuples(); row++) {
      try {
        LOGGER.info("row number" + row);
        LOGGER.info("number of tuples " + tb.numTuples());
        LOGGER.info("number of columns" + tb.numColumns());
        // write tuples to stream
        writeToStream(tb, row, tb.numColumns());
        // read results back from stream
        readFromStream(ret);
        // sendErrorbuffer(ret);
        LOGGER.info("successfully read from the stream");
      } catch (DbException e) {
        LOGGER.info("Error writing to python stream" + e.getMessage());
        throw new DbException(e);
      }
    }
    LOGGER.info("done trying to return column");
    return ret.build();
  }

  private void sendErrorbuffer(final WritableColumn output) {
    byte[] b = "1".getBytes();

    output.appendByteBuffer(ByteBuffer.wrap(b));

  }

  private void readFromStream(final WritableColumn output) throws DbException {
    LOGGER.info("trying to read now");
    int length = 0;
    DataInputStream dIn = pyWorker.getDataInputStream();

    try {
      length = dIn.readInt(); // read length of incoming message
      switch (length) {
        case -3:
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));

        default:
          if (length > 0) {
            LOGGER.info("length greater than zero!");
            byte[] obj = new byte[length];
            dIn.readFully(obj);
            output.appendByteBuffer(ByteBuffer.wrap(obj));
          }
          break;
      }

    } catch (Exception e) {
      LOGGER.info("Error reading int from stream");
      throw new DbException(e);
    }

  }

  private void writeToStream(final TupleBatch tb, final int row, final int tuplesize) throws DbException {
    List<? extends Column<?>> inputColumns = tb.getDataColumns();
    DataOutputStream dOut = pyWorker.getDataOutputStream();
    LOGGER.info("got the output stream!");

    if (tuplesize > 1) {
      LOGGER.info("tuple size = " + tuplesize);
      try {

        for (int element = 0; element < tuplesize; element++) {

          Type type = inputColumns.get(element).getType();
          LOGGER.info("column type" + type);

          switch (type) {
            case BOOLEAN_TYPE:
              dOut.writeInt(Integer.SIZE / Byte.SIZE);
              dOut.writeBoolean(inputColumns.get(element).getBoolean(row));
              break;
            case DOUBLE_TYPE:
              dOut.writeInt(Double.SIZE / Byte.SIZE);
              dOut.writeDouble(inputColumns.get(element).getDouble(row));
              break;
            case FLOAT_TYPE:
              dOut.writeInt(Float.SIZE / Byte.SIZE);
              dOut.writeFloat(inputColumns.get(element).getFloat(row));
              break;
            case INT_TYPE:
              int i = inputColumns.get(element).getInt(row);
              LOGGER.info("int type" + i);
              // dOut.writeInt(Integer.SIZE / Byte.SIZE);
              // dOut.writeInt(inputColumns.get(element).getInt(row));
              break;
            case LONG_TYPE:
              dOut.writeInt(Long.SIZE / Byte.SIZE);
              dOut.writeLong(inputColumns.get(element).getLong(row));
              break;
            case STRING_TYPE:
              StringBuilder sb = new StringBuilder();
              sb.append(inputColumns.get(element).getString(row));
              dOut.writeInt(sb.length());
              dOut.writeChars(sb.toString());
              break;
            case DATETIME_TYPE:
              LOGGER.info("date time not yet supported for python UDF ");
              break;
            case BYTES_TYPE:
              LOGGER.info("writing Bytebuffer to py process");
              ByteBuffer input = inputColumns.get(element).getByteBuffer(row);
              LOGGER.info("input array buffer length" + input.array().length);
              dOut.writeInt(input.array().length);
              dOut.write(input.array());
              break;
          }
          LOGGER.info("flushing data");
          dOut.flush();

        }

      } catch (IOException e) {
        LOGGER.info("IOException when writing to python process stream");
        // throw new DbException(e);
      }
    }

  }

}
