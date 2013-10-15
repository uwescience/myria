package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;

/**
 * Keeps min vaule. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not referenced.
 * This implementation reduces memory consumption.
 * */
public final class KeepMinValue extends StreamingStateUpdater {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(KeepMinValue.class.getName());

  /**
   * Indices to unique tuples.
   * */
  private transient HashMap<Integer, List<Integer>> uniqueTupleIndices;

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
   * @param cntTuple a list representation of a tuple to compare
   * @return true if equals.
   * */
  private boolean tupleEquals(final int index, final List<Object> cntTuple) {
    for (int i : keyColIndices) {
      if (!(uniqueTuples.get(i, index)).equals(cntTuple.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param index the index in uniqueTuples
   * @param cntTuple a list representation of a tuple to compare
   * @return true if equals.
   * */
  private boolean replace(final int index, final List<Object> cntTuple) {
    Type t = uniqueTuples.getSchema().getColumnType(valueColIndex);
    switch (t) {
      case INT_TYPE:
        return ((Integer) cntTuple.get(valueColIndex)) < ((Integer) uniqueTuples.get(valueColIndex, index));
      case FLOAT_TYPE:
        return ((Float) cntTuple.get(valueColIndex)) < ((Float) uniqueTuples.get(valueColIndex, index));
      case DOUBLE_TYPE:
        return ((Double) cntTuple.get(valueColIndex)) < ((Double) uniqueTuples.get(valueColIndex, index));
      case LONG_TYPE:
        return ((Long) cntTuple.get(valueColIndex)) < ((Long) uniqueTuples.get(valueColIndex, index));
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
    final BitSet toRemove = new BitSet(numTuples);
    final List<Object> cntTuple = new ArrayList<Object>();
    for (int i = 0; i < numTuples; ++i) {
      cntTuple.clear();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = tb.hashCode(i, keyColIndices);
      List<Integer> tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      if (tupleIndexList == null) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, cntTuple.get(j));
        }
        tupleIndexList = new ArrayList<Integer>();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
        continue;
      }
      boolean unique = true;
      for (final int oldTupleIndex : tupleIndexList) {
        if (tupleEquals(oldTupleIndex, cntTuple)) {
          unique = false;
          if (replace(oldTupleIndex, cntTuple)) {
            uniqueTuples.replace(valueColIndex, oldTupleIndex, cntTuple.get(valueColIndex));
          } else {
            toRemove.set(i);
          }
          break;
        }
      }
      if (unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, cntTuple.get(j));
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
    uniqueTupleIndices = new HashMap<Integer, List<Integer>>();
    uniqueTuples = new TupleBuffer(getSchema());
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = keepMinValue(tb);
    if (newtb.numTuples() > 0) {
      return newtb;
    }
    return null;
  }
}
