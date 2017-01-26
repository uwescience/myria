package edu.washington.escience.myria.operator;

import java.io.Serializable;

import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * An abstraction of a hash table of tuples.
 */
public final class TupleHashTable implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Map from hash codes to indices. */
  private transient IntObjectHashMap<IntArrayList> keyToIndices;
  /** The table containing keys and values. */
  private transient MutableTupleBuffer data;
  /** Key column indices. */
  private final int[] keyColumns;

  /**
   * @param schema schema
   * @param keyColumns key column indices
   */
  public TupleHashTable(final Schema schema, final int[] keyColumns) {
    this.keyColumns = keyColumns;
    data = new MutableTupleBuffer(schema);
    keyToIndices = new IntObjectHashMap<IntArrayList>();
  }

  /**
   * @return the number of tuples this hash table has.
   */
  public int numTuples() {
    return data.numTuples();
  }

  /**
   * Get the data table indices given key columns from a tuple in a tuple batch.
   *
   * @param tb the input tuple batch
   * @param key the key columns
   * @param row the row index of the tuple
   * @return the indices
   */
  public IntArrayList getIndices(final TupleBatch tb, final int[] key, final int row) {
    IntArrayList ret = new IntArrayList();
    IntArrayList indices = keyToIndices.get(HashUtils.hashSubRow(tb, key, row));
    if (indices != null) {
      IntIterator iter = indices.intIterator();
      while (iter.hasNext()) {
        int i = iter.next();
        if (TupleUtils.tupleEquals(tb, key, row, data, keyColumns, i)) {
          ret.add(i);
        }
      }
    }
    return ret;
  }

  /**
   * Replace tuples in the hash table with the input tuple if they have the same key.
   *
   * @param tb the input tuple batch
   * @param keyColumns the key columns
   * @param row the row index of the input tuple
   * @return if at least one tuple is replaced
   */
  public boolean replace(final TupleBatch tb, final int[] keyColumns, final int row) {
    IntIterator iter = getIndices(tb, keyColumns, row).intIterator();
    if (!iter.hasNext()) {
      return false;
    }
    while (iter.hasNext()) {
      int i = iter.next();
      for (int j = 0; j < data.numColumns(); ++j) {
        data.replace(j, i, tb.getDataColumns().get(j), row);
      }
    }
    return true;
  }

  /**
   * @param tb tuple batch of the input tuple
   * @param keyColumns key column indices
   * @param row row index of the input tuple
   * @param keyOnly only add keyColumns
   */
  public void addTuple(
      final TupleBatch tb, final int[] keyColumns, final int row, final boolean keyOnly) {
    int hashcode = HashUtils.hashSubRow(tb, keyColumns, row);
    IntArrayList indices = keyToIndices.get(hashcode);
    if (indices == null) {
      indices = new IntArrayList();
      keyToIndices.put(hashcode, indices);
    }
    indices.add(numTuples());
    if (keyOnly) {
      for (int i = 0; i < keyColumns.length; ++i) {
        data.put(i, tb.getDataColumns().get(keyColumns[i]), row);
      }
    } else {
      for (int i = 0; i < data.numColumns(); ++i) {
        data.put(i, tb.getDataColumns().get(i), row);
      }
    }
  }

  /**
   * @return the data
   */
  public MutableTupleBuffer getData() {
    return data;
  }

  /**
   * Clean up the hash table.
   */
  public void cleanup() {
    keyToIndices = new IntObjectHashMap<IntArrayList>();
    data = new MutableTupleBuffer(data.getSchema());
  }
}
