package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.column.Column;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Duplicate elimination. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not
 * referenced. This implementation reduces memory consumption.
 * */
public final class DupElim extends StreamingStateUpdater {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(DupElim.class.getName());

  /**
   * Indices to unique tuples.
   * */
  private transient TIntObjectMap<TIntList> uniqueTupleIndices;

  /**
   * The buffer for storing unique tuples.
   * */
  private transient TupleBuffer uniqueTuples = null;

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
  }

  /**
   * Do duplicate elimination for tb.
   * 
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch doDupElim(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    /* if tb is empty, directly return. */
    if (numTuples <= 0) {
      return tb;
    }
    final BitSet toRemove = new BitSet(numTuples);
    final List<Column<?>> tbColumns = tb.getDataColumns();
    for (int row = 0; row < numTuples; ++row) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = tb.hashCode(row);
      TIntList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      int inColumnRow = tb.getValidIndices().get(row);

      /* update hash table if the hash entry of the hash value of this tuple does not exist. */
      if (tupleIndexList == null) {
        for (int column = 0; column < tb.numColumns(); ++column) {
          uniqueTuples.put(column, tbColumns.get(column), inColumnRow);
        }
        tupleIndexList = new TIntArrayList();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
        continue;
      }

      /* detect is there a equal tuple existing. */
      boolean unique = true;
      for (int i = 0; i < tupleIndexList.size(); ++i) {
        if (tb.tupleEquals(row, uniqueTuples, tupleIndexList.get(i))) {
          unique = false;
          break;
        }
      }

      /* update the hash table if current tuple is unique, delete it otherwise. */
      if (unique) {
        for (int column = 0; column < tb.numColumns(); ++column) {
          uniqueTuples.put(column, tbColumns.get(column), inColumnRow);
        }
        tupleIndexList.add(nextIndex);
      } else {
        toRemove.set(row);
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
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = doDupElim(tb);
    if (newtb.numTuples() > 0) {
      return newtb;
    }
    return null;
  }
}
