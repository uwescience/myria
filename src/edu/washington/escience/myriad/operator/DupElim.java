package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class DupElim extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private class IndexedTuple {
    int index;
    _TupleBatch tb;

    public IndexedTuple(final _TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    public boolean compareField(final IndexedTuple another, final int colIndx) {
      final int rowIndx1 = index;
      final int rowIndx2 = another.index;
      return tb.getObject(colIndx, rowIndx1).equals(another.tb.getObject(colIndx, rowIndx2));
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      if (!(tb.inputSchema().equals(another.tb.inputSchema()))) {
        return false;
      }
      for (int i = 0; i < tb.inputSchema().numFields(); ++i) {
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

  Operator child;
  HashMap<Integer, List<IndexedTuple>> uniqueTuples;

  public DupElim(final Operator child) {
    this.child = child;
    uniqueTuples = new HashMap<Integer, List<IndexedTuple>>();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    _TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      tb = doDupElim(tb);
      if (tb.numOutputTuples() > 0) {
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

  protected _TupleBatch doDupElim(_TupleBatch tb) {
    for (int i = 0; i < tb.numInputTuples(); ++i) {
      final IndexedTuple cntTuple = new IndexedTuple(tb, i);
      final int cntHashCode = cntTuple.hashCode();
      // might need to check invalid | change to use outputTuples later
      if (uniqueTuples.get(cntHashCode) == null) {
        uniqueTuples.put(cntHashCode, new ArrayList<IndexedTuple>());
      }
      final List<IndexedTuple> tupleList = uniqueTuples.get(cntHashCode);
      boolean unique = true;
      for (int j = 0; j < tupleList.size(); ++j) {
        final IndexedTuple oldTuple = tupleList.get(j);
        if (cntTuple.equals(oldTuple)) {
          unique = false;
          break;
        }
      }
      System.out.println(i + " " + unique);
      if (unique) {
        tupleList.add(cntTuple);
      } else {
        tb.remove(i);
      }
    }
    return tb;
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    _TupleBatch tb = null;
    while (!eos() && child.nextReady()) {
      tb = child.next();
      tb = doDupElim(tb);
      if (tb.numOutputTuples() > 0) {
        return tb;
      } else {
        return null;
      }
    }
    return null;
  }
}
