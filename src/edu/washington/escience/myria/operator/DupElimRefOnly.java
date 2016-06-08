package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * A simple implementation of duplicate eliminate. It keeps the references to all the TupleBatches which contain unique
 * tuples.
 * */
public final class DupElimRefOnly extends UnaryOperator {

  /**
   * Pointer data structure for pointing to a tuple in a TupleBatch.
   * */
  private class IndexedTuple {
    /**
     * The row index.
     * */
    private int index;
    /**
     * The source data TB.
     * */
    private final TupleBatch tb;

    /**
     * @param tb the source data TB.
     * */
    public IndexedTuple(final TupleBatch tb) {
      this.tb = tb;
    }

    /**
     * @param tb the source data TB.
     * @param index the row index.
     * */
    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    /**
     * compare the equality of a column of two tuples.
     *
     * @return true if equal.
     * @param another another source data TB.
     * @param colIndx columnIndex to compare
     * */
    public boolean columnEquals(final IndexedTuple another, final int colIndx) {
      final Type type = tb.getSchema().getColumnType(colIndx);
      final int rowIndx1 = index;
      final int rowIndx2 = another.index;
      switch (type) {
        case BOOLEAN_TYPE:
          return tb.getBoolean(colIndx, rowIndx1) == another.tb.getBoolean(colIndx, rowIndx2);
        case DOUBLE_TYPE:
          return tb.getDouble(colIndx, rowIndx1) == another.tb.getDouble(colIndx, rowIndx2);
        case FLOAT_TYPE:
          return tb.getFloat(colIndx, rowIndx1) == another.tb.getFloat(colIndx, rowIndx2);
        case INT_TYPE:
          return tb.getInt(colIndx, rowIndx1) == another.tb.getInt(colIndx, rowIndx2);
        case LONG_TYPE:
          return tb.getLong(colIndx, rowIndx1) == another.tb.getLong(colIndx, rowIndx2);
        case STRING_TYPE:
          return tb.getString(colIndx, rowIndx1).equals(another.tb.getString(colIndx, rowIndx2));
        case DATETIME_TYPE:
          return tb.getDateTime(colIndx, rowIndx1)
              .equals(another.tb.getDateTime(colIndx, rowIndx2));
      }
      return false;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      if (!(tb.getSchema().equals(another.tb.getSchema()))) {
        return false;
      }
      for (int i = 0; i < tb.getSchema().numColumns(); ++i) {
        if (!columnEquals(another, i)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return HashUtils.hashRow(tb, index);
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Storing the unique tuples.
   * */
  private transient HashMap<Integer, List<IndexedTuple>> uniqueTuples;

  /**
   * @param child the child
   * */
  public DupElimRefOnly(final Operator child) {
    super(child);
  }

  @Override
  protected void cleanup() throws DbException {}

  /**
   * Do duplicate elimination for the tb.
   *
   * @param tb the TB.
   * @return a new TB with duplicates removed.
   * */
  protected TupleBatch doDupElim(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    final BitSet toRemove = new BitSet(numTuples);
    final IndexedTuple currentTuple = new IndexedTuple(tb);
    for (int i = 0; i < numTuples; ++i) {
      currentTuple.index = i;
      final int cntHashCode = currentTuple.hashCode();
      // might need to check invalid | change to use outputTuples later
      List<IndexedTuple> tupleList = uniqueTuples.get(cntHashCode);
      if (tupleList == null) {
        tupleList = new ArrayList<IndexedTuple>();
        uniqueTuples.put(cntHashCode, tupleList);
        tupleList.add(new IndexedTuple(tb, i));
        continue;
      }
      boolean unique = true;
      for (final IndexedTuple oldTuple : tupleList) {
        if (currentTuple.equals(oldTuple)) {
          unique = false;
          break;
        }
      }
      if (unique) {
        tupleList.add(new IndexedTuple(tb, i));
      } else {
        toRemove.set(i);
      }
    }
    return tb.filterOut(toRemove);
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {

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
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    uniqueTuples = new HashMap<Integer, List<IndexedTuple>>();
  }
}
