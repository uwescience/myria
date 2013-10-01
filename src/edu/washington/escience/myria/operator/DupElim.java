package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBuffer;

/**
 * Duplicate elimination. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not
 * referenced. This implementation reduces memory consumption.
 * */
public final class DupElim extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Indices to unique tuples.
   * */
  private transient HashMap<Integer, List<Integer>> uniqueTupleIndices;

  /**
   * The buffer for stroing unique tuples.
   * */
  private transient TupleBuffer uniqueTuples = null;

  /**
   * @param child the child.
   * */
  public DupElim(final Operator child) {
    super(child);
  }

  @Override
  protected void cleanup() throws DbException {
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
    for (int i = 0; i < cntTuple.size(); ++i) {
      if (!(uniqueTuples.get(i, index)).equals(cntTuple.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Do duplicate elimination for tb.
   * 
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch doDupElim(final TupleBatch tb) {
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
      final int cntHashCode = tb.hashCode(i);
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
          break;
        }
      }
      if (unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, cntTuple.get(j));
        }
        tupleIndexList.add(nextIndex);
      } else {
        toRemove.set(i);
      }
    }
    return tb.remove(toRemove);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    tb = getChild().nextReady();
    while (tb != null) {
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        return tb;
      }
      tb = getChild().nextReady();
    }
    return null;
  }

  @Override
  public Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    uniqueTupleIndices = new HashMap<Integer, List<Integer>>();
    uniqueTuples = new TupleBuffer(getSchema());
  }
}
