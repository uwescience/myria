package edu.washington.escience.myriad.operator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;

public final class DupElim extends Operator implements Externalizable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private Operator child;

  private HashMap<Integer, List<Integer>> uniqueTupleIndices;
  private List<Column<?>> uniqueTuples = null;

  public DupElim() {
  }

  public DupElim(final Operator child) {
    this.child = child;
  }

  private boolean compareTuple(final int index, final List<Object> cntTuple) {
    for (int i = 0; i < cntTuple.size(); ++i) {
      if (!(uniqueTuples.get(i).get(index)).equals(cntTuple.get(i))) {
        return false;
      }
    }
    return true;
  }

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
      final int nextIndex = uniqueTuples.get(0).size();
      final int cntHashCode = tb.hashCode(i);
      List<Integer> tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      if (tupleIndexList == null) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.get(j).putObject(cntTuple.get(j));
        }
        tupleIndexList = new ArrayList<Integer>();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
        continue;
      }
      boolean unique = true;
      for (final int oldTupleIndex : tupleIndexList) {
        if (compareTuple(oldTupleIndex, cntTuple)) {
          unique = false;
          break;
        }
      }
      if (unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.get(j).putObject(cntTuple.get(j));
        }
        tupleIndexList.add(nextIndex);
      } else {
        toRemove.set(i);
      }
    }
    return tb.remove(toRemove);
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
  protected void cleanup() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    child = (Operator) in.readObject();
    uniqueTupleIndices = new HashMap<Integer, List<Integer>>();
    uniqueTuples = ColumnFactory.allocateColumns(getSchema());
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(child);
  }
}
