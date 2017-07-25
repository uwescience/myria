package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * An abstraction of a hash table of tuples.
 */
public class TupleHashTable implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Map from hash codes to indices. */
  private transient IntObjectHashMap<IntArrayList> keyHashCodesToIndices;
  /** The table containing keys and values. */
  protected transient MutableTupleBuffer data;
  /** Key column indices. */
  protected final int[] keyColumns;
  /* Name of the hash table. */
  public String name;
  /* Stats. */
  protected Map<String, Integer> stats;

  /**
   * @param schema schema
   * @param keyColumns key column indices
   */
  public TupleHashTable(final Schema schema, final int[] keyColumns) {
    this.keyColumns = keyColumns;
    data = new MutableTupleBuffer(schema);
    keyHashCodesToIndices = new IntObjectHashMap<IntArrayList>();
    int numIntegers = 0, numLongs = 0, numStrings = 0;
    for (Type t : schema.getColumnTypes()) {
      if (t == Type.INT_TYPE || t == Type.FLOAT_TYPE) {
        numIntegers += 1;
      }
      if (t == Type.LONG_TYPE || t == Type.DOUBLE_TYPE) {
        numLongs += 1;
      }
      if (t == Type.STRING_TYPE) {
        numStrings += 1;
      }
    }
    stats = new HashMap<String, Integer>();
    stats.put("numIntegers", numIntegers);
    stats.put("numLongs", numLongs);
    stats.put("numStrings", numStrings);
    stats.put("sumStrings", 0);
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
  public IntArrayList getIndices(final ReadableTable tb, final int[] key, final int row) {
    IntArrayList ret = new IntArrayList();
    IntArrayList indices = keyHashCodesToIndices.get(HashUtils.hashSubRow(tb, key, row));
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
        if (tb.getSchema().getColumnType(j) == Type.STRING_TYPE) {
          stats.put(
              "sumStrings",
              stats.get("sumStrings")
                  - tb.getString(j, i).length()
                  + tb.getString(j, row).length());
        }
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
      final ReadableTable tb, final int[] keyColumns, final int row, final boolean keyOnly) {
    int hashcode = HashUtils.hashSubRow(tb, keyColumns, row);
    IntArrayList indices = keyHashCodesToIndices.get(hashcode);
    if (indices == null) {
      indices = new IntArrayList();
      keyHashCodesToIndices.put(hashcode, indices);
    }
    indices.add(numTuples());
    if (keyOnly) {
      for (int i = 0; i < keyColumns.length; ++i) {
        if (tb.getSchema().getColumnType(i) == Type.STRING_TYPE) {
          stats.put(
              "sumStrings", stats.get("sumStrings") + tb.getString(keyColumns[i], row).length());
        }
        data.put(i, tb.asColumn(keyColumns[i]), row);
      }
    } else {
      for (int i = 0; i < data.numColumns(); ++i) {
        if (tb.getSchema().getColumnType(i) == Type.STRING_TYPE) {
          stats.put("sumStrings", stats.get("sumStrings") + tb.getString(i, row).length());
        }
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
    keyHashCodesToIndices = new IntObjectHashMap<IntArrayList>();
    data = new MutableTupleBuffer(data.getSchema());
  }

  /**
   *
   * @return stats of the hash table.
   */
  public Map<String, Integer> dumpStats() {
    Map<String, Integer> ret = new HashMap<String, Integer>(stats);
    ret.put("numTuples", numTuples());
    ret.put("numKeys", keyHashCodesToIndices.size());
    return ret;
  }

  /**
   *
   * @return the schema.
   */
  public Schema getSchema() {
    return data.getSchema();
  }
}
