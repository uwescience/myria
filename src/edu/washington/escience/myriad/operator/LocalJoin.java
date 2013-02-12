package edu.washington.escience.myriad.operator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;

public final class LocalJoin extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;
  private final Schema outputSchema;
  private final int[] compareIndx1;
  private final int[] compareIndx2;
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  private transient List<Column<?>> hashTable1;
  private transient List<Column<?>> hashTable2;
  private transient TupleBatchBuffer ans;

  public LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
  }

  protected void addToAns(final List<Object> cntTuple, final List<Column<?>> hashTable, final int index,
      final boolean fromChild1) {
    final int offset1 = (fromChild1 ? 0 : hashTable.size());
    final int offset2 = (fromChild1 ? cntTuple.size() : 0);
    for (int i = 0; i < cntTuple.size(); ++i) {
      ans.put(i + offset1, cntTuple.get(i));
    }
    for (int i = 0; i < hashTable.size(); ++i) {
      ans.put(i + offset2, hashTable.get(i).get(index));
    }
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasNewTuple = false;
      TupleBatch tb = null;
      if ((tb = child1.next()) != null) {
        hasNewTuple = true;
        processChildTB(tb, hashTable1, hashTable2, hashTable1Indices, hashTable2Indices, compareIndx1, compareIndx2,
            true);
      }
      if ((tb = child2.next()) != null) {
        hasNewTuple = true;
        processChildTB(tb, hashTable2, hashTable1, hashTable2Indices, hashTable1Indices, compareIndx2, compareIndx1,
            false);
      }
      nexttb = ans.popFilled();
      if (!hasNewTuple) {
        break;
      }
    }
    if (nexttb == null) {
      if (ans.numTuples() > 0) {
        nexttb = ans.popAny();
      }
    }
    return nexttb;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init() throws DbException {
  }

  private boolean compareTuple(final List<Object> cntTuple, final List<Column<?>> hashTable, final int index,
      final int[] compareIndx1, final int[] compareIndx2) {
    if (compareIndx1.length != compareIndx2.length) {
      return false;
    }
    for (int i = 0; i < compareIndx1.length; ++i) {
      if (!cntTuple.get(compareIndx1[i]).equals(hashTable.get(compareIndx2[i]).get(index))) {
        return false;
      }
    }
    return true;
  }

  protected void processChildTB(final TupleBatch tb, final List<Column<?>> hashTable1,
      final List<Column<?>> hashTable2, final HashMap<Integer, List<Integer>> hashTable1Indices,
      final HashMap<Integer, List<Integer>> hashTable2Indices, final int[] compareIndx1, final int[] compareIndx2,
      final boolean fromChild1) {

    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = hashTable1.get(0).size();
      final int cntHashCode = tb.hashCode(i, compareIndx1);
      List<Integer> indexList = hashTable2Indices.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (compareTuple(cntTuple, hashTable2, index, compareIndx1, compareIndx2)) {
            addToAns(cntTuple, hashTable2, index, fromChild1);
          }
        }
      }
      if (hashTable1Indices.get(cntHashCode) == null) {
        hashTable1Indices.put(cntHashCode, new ArrayList<Integer>());
      }
      hashTable1Indices.get(cntHashCode).add(nextIndex);
      for (int j = 0; j < tb.numColumns(); ++j) {
        hashTable1.get(j).putObject(cntTuple.get(j));
      }

    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

  private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    hashTable1 = ColumnFactory.allocateColumns(child1.getSchema());
    hashTable2 = ColumnFactory.allocateColumns(child2.getSchema());
    ans = new TupleBatchBuffer(getSchema());
  }

  private void writeObject(final ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

}
