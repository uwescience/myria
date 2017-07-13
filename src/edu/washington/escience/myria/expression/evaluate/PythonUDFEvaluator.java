/**
 *
 */
package edu.washington.escience.myria.expression.evaluate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.codehaus.janino.ExpressionEvaluator;

import com.google.common.base.Preconditions;
import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.FunctionStatus;
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
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBuffer;

/**
 * An Expression evaluator for Python UDFs. Used in {@link Apply} and {@link StatefulApply}.
 */
public class PythonUDFEvaluator extends GenericEvaluator {

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PythonUDFEvaluator.class);
  /** python function registrar from which to fetch function pickle. */
  private final PythonFunctionRegistrar pyFuncRegistrar;
  /** python worker process. */
  private PythonWorker pyWorker;
  /** index of state column. */
  private Set<Integer> stateColumns;
  /** column indices of child ops. */
  private int[] columnIdxs = null;
  /** Output Type of the expression. */
  private Type outputType = null;
  /** is expression a flatmap? */
  private Boolean isMultiValued = false;
  /** Tuple buffers for each group key. */
  private TupleBuffer buffer;
  /** Mapping from row indices of state to rows in {@link PythonUDFEvaluator#buffer}. */
  private IntObjectHashMap<IntArrayList> groups;
  /** The internal state schema. */
  private Schema stateSchema;

  /**
   * Default constructor.
   *
   * @param expression the expression for the evaluator
   * @param parameters parameters that are passed to the expression
   * @param pyFuncReg python function registrar to get the python function.
   */
  public PythonUDFEvaluator(
      final Expression expression, final ExpressionOperatorParameter parameters)
      throws DbException {
    super(expression, parameters);

    pyFuncRegistrar = parameters.getPythonFunctionRegistrar();
    if (pyFuncRegistrar == null) {
      throw new RuntimeException("PythonRegistrar should not be null in PythonUDFEvaluator.");
    }
    PyUDFExpression op = (PyUDFExpression) expression.getRootExpressionOperator();
    outputType = op.getOutputType(parameters);
    List<ExpressionOperator> childops = op.getChildren();
    columnIdxs = new int[childops.size()];
    stateColumns = new HashSet<Integer>();
    List<Type> types = new ArrayList<Type>();
    for (int i = 0; i < childops.size(); i++) {
      if (childops.get(i) instanceof StateExpression) {
        stateColumns.add(i);
        columnIdxs[i] = ((StateExpression) childops.get(i)).getColumnIdx();
        types.add(((StateExpression) childops.get(i)).getOutputType(parameters));
      } else if (childops.get(i) instanceof VariableExpression) {
        columnIdxs[i] = ((VariableExpression) childops.get(i)).getColumnIdx();
        types.add(((VariableExpression) childops.get(i)).getOutputType(parameters));
      } else {
        throw new IllegalStateException(
            "Python expression can only have State or Variable expression as child expressions.");
      }
    }
    stateSchema = new Schema(types);

    String pyFunctionName = op.getName();
    FunctionStatus fs = pyFuncRegistrar.getFunctionStatus(pyFunctionName);
    if (fs == null) {
      throw new DbException("No Python UDF with name " + pyFunctionName + " is registered.");
    }
    isMultiValued = fs.getIsMultiValued();
    pyWorker = new PythonWorker();
    pyWorker.sendCodePickle(fs.getBinary(), columnIdxs.length, outputType, isMultiValued);
    buffer = new TupleBuffer(stateSchema);
    groups = new IntObjectHashMap<IntArrayList>();
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
      @Nonnull final ReadableTable input,
      final int inputRow,
      @Nullable final ReadableTable state,
      final int stateRow,
      @Nonnull final WritableColumn result,
      @Nullable final WritableColumn count)
      throws DbException, BufferOverflowException, IOException {
    pyWorker.sendNumTuples(1);
    for (int i = 0; i < columnIdxs.length; ++i) {
      if (stateColumns.contains(i)) {
        writeToStream(state, stateRow, columnIdxs[i]);
      } else {
        writeToStream(input, inputRow, columnIdxs[i]);
      }
    }
    readFromStream(count, result);
  }

  @Override
  public void updateState(
      @Nonnull final ReadableTable input,
      final int inputRow,
      @Nonnull final MutableTupleBuffer state,
      final int stateRow,
      final int stateColOffset)
      throws DbException {
    if (!groups.containsKey(stateRow)) {
      groups.put(stateRow, new IntArrayList());
    }
    for (int i = 0; i < columnIdxs.length; ++i) {
      if (stateColumns.contains(i)) {
        buffer.put(i, state.asColumn(columnIdxs[i] + stateColOffset), stateRow);
      } else {
        buffer.put(i, input.asColumn(columnIdxs[i]), inputRow);
      }
    }
    IntArrayList indices = groups.get(stateRow);
    indices.add(buffer.numTuples() - 1);
  };

  /**
   * @param state state
   * @param col column index of the state to be written to.
   * @throws DbException in case of error
 * @throws IOException 
 * @throws BufferOverflowException 
   */
  public void evalGroups(final MutableTupleBuffer state, final int col) throws DbException, BufferOverflowException, IOException {
    IntIterator keyIter = groups.keySet().intIterator();
    while (keyIter.hasNext()) {
      int key = keyIter.next();
      pyWorker.sendNumTuples(groups.get(key).size());
      IntIterator rowIter = groups.get(key).intIterator();
      while (rowIter.hasNext()) {
        int row = rowIter.next();
        for (int i = 0; i < buffer.numColumns(); ++i) {
          writeToStream(buffer, row, i);
        }
      }
      ColumnBuilder<?> output = ColumnFactory.allocateColumn(outputType);
      /* TODO: Leaving the count column to be null for now since since it's not used by Python evaluator for aggregate.
       * A better design is to let the Aggregator emit two columns or even multiple columns. */
      readFromStream(null, output);
      if (output.size() > 1) {
        throw new RuntimeException("PythonUDFEvaluator cannot be multivalued for Aggregate");
      }
      for (int i = 0; i < output.size(); ++i) {
        state.replace(col, key, output, i);
      }
    }
  }

  /**
   * @param count number of tuples returned.
   * @param result writable column
   * @param result2 appendable table
   * @param resultColIdx id of the result column.
   * @throws DbException in case of error.
 * @throws BufferOverflowException 
 * @throws IOException 
   */
  public void readFromStream(final WritableColumn count, final WritableColumn result)
      throws DbException, BufferOverflowException, IOException {
    DataInputStream dIn = pyWorker.getDataInputStream();
    int c = 1; // single valued expressions only return 1 tuple.
    //try {
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
        // then read the type of tuple
        int type = dIn.readInt();
        // Signals that an exception has been thrown in python
        if (type == MyriaConstants.PythonSpecialLengths.PYTHON_EXCEPTION.getVal()) {
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp,StandardCharsets.UTF_8));

        } else {
          // read the rest of the tuple
          if (type == MyriaConstants.PythonType.DOUBLE.getVal()) {
            result.appendDouble(dIn.readDouble());
          } else if (type == MyriaConstants.PythonType.FLOAT.getVal()) {
            result.appendFloat(dIn.readFloat());
          } else if (type == MyriaConstants.PythonType.INT.getVal()) {
            result.appendInt(dIn.readInt());
          } else if (type == MyriaConstants.PythonType.LONG.getVal()) {
            result.appendLong(dIn.readLong());
          } else if (type == MyriaConstants.PythonType.BLOB.getVal()) {
            int l = dIn.readInt();
            if (l > 0) {
              byte[] obj = new byte[l];
              dIn.readFully(obj);
              result.appendBlob(ByteBuffer.wrap(obj));
            }
          } else {
            throw new DbException("Type not supported by python");
          }
        }
      }
    //} catch (Exception e) {
      
    //}
  }

  /**
   * helper function to write to python process.
   *
   * @param tb - input tuple buffer.
   * @param row - row being evaluated.
   * @param columnIdx - column to be written to the py process.
   * @throws DbException in case of error.
   */
  private void writeToStream(@Nonnull final ReadableTable tb, final int row, final int columnIdx)
      throws DbException {
    DataOutputStream dOut = pyWorker.getDataOutputStream();
    Preconditions.checkNotNull(tb, "input tuple cannot be null");
    Preconditions.checkNotNull(dOut, "Output stream for python process cannot be null");
    try {
      switch (tb.getSchema().getColumnType(columnIdx)) {
        case BOOLEAN_TYPE:
          LOGGER.debug("BOOLEAN type not supported for python function");
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
