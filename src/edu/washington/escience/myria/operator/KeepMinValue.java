package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.LongColumn;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;

import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Keeps min vaule. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not referenced.
 * This implementation reduces memory consumption.
 * */
public final class KeepMinValue extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(KeepMinValue.class.getName());

  /**
   * Indices to unique tuples.
   * */
  private transient TIntObjectMap<TIntList> uniqueTupleIndices;

  /**
   * The buffer for stroing unique tuples.
   * */
  private transient TupleBuffer uniqueTuples = null;

  /** column indices of the key. */
  private final int[] keyColIndices;
  /** column indices of the value. */
  private final int valueColIndex;

  /**
   * 
   * @param keyColIndices column indices of the key
   * @param valueColIndex column index of the value
   */
  public KeepMinValue(final int[] keyColIndices, final int valueColIndex) {
    this.keyColIndices = keyColIndices;
    this.valueColIndex = valueColIndex;
  }

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param index the index in uniqueTuples
   * @param column the source column
   * @param row the index of the source row
   * @return true if equals.
   * */
  private boolean shouldReplace(final int index, final Column<?> column, final int row) {
    Type t = column.getType();
    switch (t) {
      case INT_TYPE:
        return (((IntColumn) column).get(row)) < uniqueTuples.getInt(valueColIndex, index);
      case FLOAT_TYPE:
        return (((FloatColumn) column).get(row)) < uniqueTuples.getFloat(valueColIndex, index);
      case DOUBLE_TYPE:
        return (((DoubleColumn) column).get(row)) < uniqueTuples.getDouble(valueColIndex, index);
      case LONG_TYPE:
        return (((LongColumn) column).get(row)) < uniqueTuples.getLong(valueColIndex, index);
      default:
        throw new IllegalStateException("type " + t + " is not supported in KeepMinValue.replace()");
    }
  }

  /**
   * Do duplicate elimination for tb.
   * 
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch keepMinValue(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    doReplace.inputTB = tb;
    final List<Column<?>> columns = tb.getDataColumns();
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = tb.hashCode(i, keyColIndices);
      TIntList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      doReplace.unique = true;
      if (tupleIndexList == null) {
        tupleIndexList = new TIntArrayList();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
      } else {
        doReplace.replaced = false;
        doReplace.row = i;
        tupleIndexList.forEach(doReplace);
        if (!doReplace.unique && !doReplace.replaced) {
          toRemove.set(i);
        }
      }
      if (doReplace.unique) {
        int inColumnRow = tb.getValidIndices().get(i);
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, columns.get(j), inColumnRow);
        }
        tupleIndexList.add(nextIndex);
      }
    }
    return tb.remove(toRemove);
  }

  @Override
  public Schema getSchema() {
    return op.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    uniqueTupleIndices = new TIntObjectHashMap<TIntList>();
    uniqueTuples = new TupleBuffer(getSchema());
    doReplace = new ReplaceProcedure();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = keepMinValue(tb);
    if (newtb.numTuples() > 0) {
      return newtb;
    }
    return null;
  }

  @Override
  public List<TupleBatch> exportState() {
    return uniqueTuples.getAll();
  }

  /**
   * Traverse through the list of tuples and replace old values.
   * */
  private transient ReplaceProcedure doReplace;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class ReplaceProcedure implements TIntProcedure {

    /** row index of the tuple. */
    private int row;

    /** input TupleBatch. */
    private TupleBatch inputTB;

    /** if found a replacement. */
    private boolean replaced;

    /** if the given tuple doesn't exist. */
    private boolean unique;

    @Override
    public boolean execute(final int index) {
      if (inputTB.tupleEquals(row, uniqueTuples, index, keyColIndices, keyColIndices)) {
        unique = false;
        Column<?> valueColumn = inputTB.getDataColumns().get(valueColIndex);
        int inColumnRow = inputTB.getValidIndices().get(row);
        if (shouldReplace(index, valueColumn, inColumnRow)) {
          uniqueTuples.replace(valueColIndex, index, valueColumn, inColumnRow);
          replaced = true;
        }
      }
      return unique;
    }
  };
}
