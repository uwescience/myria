package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

public final class DupElim extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private class IndexedTuple {
    private int index;
    private final TupleBatch tb;

    public IndexedTuple(TupleBatch tb) {
      this.tb = tb;
    }

    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    public boolean compareField(final IndexedTuple another, final int colIndx) {
      final Type type = tb.getSchema().getFieldType(colIndx);
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
      }
      return false;
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      if (!(tb.getSchema().equals(another.tb.getSchema()))) {
        return false;
      }
      for (int i = 0; i < tb.getSchema().numFields(); ++i) {
        if (!compareField(another, i)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return tb.hashCode(index);
    }
  }

  private Operator child;
  private final HashMap<Integer, List<IndexedTuple>> uniqueTuples;

  public DupElim(final Operator child) {
    this.child = child;
    uniqueTuples = new HashMap<Integer, List<IndexedTuple>>();
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        return tb;
      }
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void cleanup() throws DbException {
  }

  protected TupleBatch doDupElim(TupleBatch tb) {
    int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    BitSet toRemove = new BitSet(numTuples);
    IndexedTuple currentTuple = new IndexedTuple(tb);
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
      for (IndexedTuple oldTuple : tupleList) {
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
    tb.remove(toRemove);
    return tb;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    while (!eos() && child.nextReady()) {
      tb = child.next();
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        return tb;
      } else {
        return null;
      }
    }
    return null;
  }
}
