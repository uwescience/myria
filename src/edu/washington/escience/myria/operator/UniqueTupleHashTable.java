package edu.washington.escience.myria.operator;

import java.io.Serializable;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongIntHashMap;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * An abstraction of a hash table of unique tuples.
 */
public final class UniqueTupleHashTable implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * We store this value instead of a valid index to indicate
   * that a given hash code is mapped to multiple indexes.
   */
  private static final int COLLIDING_KEY = -1;
  /**
   * We return this value from getIfAbsent() to indicate absence,
   * since 0 and -1 are already legitimate values.
   */
  private static final int ABSENT_VALUE = -2;

  /** Map from unique hash codes to indexes. */
  private transient LongIntHashMap keyHashCodesToIndexes;
  /** Map from colliding hash codes to indexes. */
  private transient LongObjectHashMap<IntArrayList> collidingKeyHashCodesToIndexes;
  /** The table containing keys and values. */
  private transient MutableTupleBuffer data;
  /** Key column indices. */
  private final int[] keyColumns;

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(UniqueTupleHashTable.class);

  /**
   * @param schema schema
   * @param keyColumns key column indices
   */
  public UniqueTupleHashTable(final Schema schema, final int[] keyColumns) {
    this.keyColumns = keyColumns;
    data = new MutableTupleBuffer(schema);
    keyHashCodesToIndexes = new LongIntHashMap();
    collidingKeyHashCodesToIndexes = new LongObjectHashMap<IntArrayList>();
  }

  /**
   * @return the number of tuples this hash table has.
   */
  public int numTuples() {
    return data.numTuples();
  }

  /**
   * Get the data table index given key columns from a tuple in a tuple batch.
   *
   * @param tb the input tuple batch
   * @param key the key columns
   * @param row the row index of the tuple
   * @return the index of the matching tuple in the data table, or -1 if no match
   */
  public int getIndex(final ReadableTable tb, final int[] key, final int row) {
    final long hashcode = HashUtils.hashSubRowLong(tb, key, row);
    int index = keyHashCodesToIndexes.getIfAbsent(hashcode, ABSENT_VALUE);
    if (index == ABSENT_VALUE) {
      return -1;
    }
    if (index == COLLIDING_KEY) {
      IntArrayList collidingIndexes = collidingKeyHashCodesToIndexes.get(hashcode);
      Preconditions.checkNotNull(collidingIndexes);
      Preconditions.checkState(collidingIndexes.size() > 1);
      IntIterator iter = collidingIndexes.intIterator();
      while (iter.hasNext()) {
        int idx = iter.next();
        if (TupleUtils.tupleEquals(tb, key, row, data, keyColumns, idx)) {
          return idx;
        }
      }
      return -1; // our search key doesn't exist but collides with an existing key
    }
    if (TupleUtils.tupleEquals(tb, key, row, data, keyColumns, index)) {
      return index;
    }
    return -1; // our search key doesn't exist but collides with an existing key
  }

  /**
   * Replace a matching tuple in the data table with the input tuple.
   *
   * @param tb the input tuple batch
   * @param keyColumns the key columns
   * @param row the row index of the input tuple
   * @return if at least one tuple is replaced
   */
  public boolean replace(final TupleBatch tb, final int[] keyColumns, final int row) {
    int index = getIndex(tb, keyColumns, row);
    if (index == -1) {
      return false;
    }
    for (int j = 0; j < data.numColumns(); ++j) {
      data.replace(j, index, tb.getDataColumns().get(j), row);
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
      final ReadableTable tb, final int[] keyColumns, final int row, final boolean keyOnly) {
    final long hashcode = HashUtils.hashSubRowLong(tb, keyColumns, row);
    int index = keyHashCodesToIndexes.getIfAbsent(hashcode, ABSENT_VALUE);
    if (index == ABSENT_VALUE) {
      keyHashCodesToIndexes.put(hashcode, numTuples());
    } else if (index == COLLIDING_KEY) {
      IntArrayList collidingIndexes = collidingKeyHashCodesToIndexes.get(hashcode);
      Preconditions.checkNotNull(collidingIndexes);
      Preconditions.checkState(collidingIndexes.size() > 1);
      collidingIndexes.add(numTuples());
    } else {
      LOGGER.warn("Collision detected with {} elements in table!", numTuples());
      IntArrayList collidingIndexes = IntArrayList.newListWith(index, numTuples());
      Preconditions.checkState(!collidingKeyHashCodesToIndexes.containsKey(hashcode));
      collidingKeyHashCodesToIndexes.put(hashcode, collidingIndexes);
      keyHashCodesToIndexes.put(hashcode, COLLIDING_KEY);
    }
    if (keyOnly) {
      for (int i = 0; i < keyColumns.length; ++i) {
        data.put(i, tb.asColumn(keyColumns[i]), row);
      }
    } else {
      for (int i = 0; i < data.numColumns(); ++i) {
        data.put(i, tb.asColumn(i), row);
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
    keyHashCodesToIndexes = new LongIntHashMap();
    collidingKeyHashCodesToIndexes = new LongObjectHashMap<IntArrayList>();
    data = new MutableTupleBuffer(data.getSchema());
  }
}
