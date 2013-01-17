package edu.washington.escience.myriad.operator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

public final class LocalJoin extends Operator implements Externalizable {

  private class IndexedTuple {
    private final int index;
    private final TupleBatch tb;

    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    public boolean compareField(final IndexedTuple another, final int colIndx1, final int colIndx2) {
      final Type type1 = tb.getSchema().getFieldType(colIndx1);
      // type check in query plan?
      final int rowIndx1 = index;
      final int rowIndx2 = another.index;
      switch (type1) {
        case INT_TYPE:
          return tb.getInt(colIndx1, rowIndx1) == another.tb.getInt(colIndx2, rowIndx2);
        case DOUBLE_TYPE:
          return tb.getDouble(colIndx1, rowIndx1) == another.tb.getDouble(colIndx2, rowIndx2);
        case STRING_TYPE:
          return tb.getString(colIndx1, rowIndx1).equals(another.tb.getString(colIndx2, rowIndx2));
        case FLOAT_TYPE:
          return tb.getFloat(colIndx1, rowIndx1) == another.tb.getFloat(colIndx2, rowIndx2);
        case BOOLEAN_TYPE:
          return tb.getBoolean(colIndx1, rowIndx1) == another.tb.getBoolean(colIndx2, rowIndx2);
        case LONG_TYPE:
          return tb.getLong(colIndx1, rowIndx1) == another.tb.getLong(colIndx2, rowIndx2);
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
        if (!compareField(another, i, i)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return tb.hashCode(index);
    }

    public int hashCode4Keys(final int[] colIndx) {
      return tb.hashCode(index, colIndx);
    }

    public boolean joinEquals(final Object o, final int[] compareIndx1, final int[] compareIndx2) {
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      if (compareIndx1.length != compareIndx2.length) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      for (int i = 0; i < compareIndx1.length; ++i) {
        if (!compareField(another, compareIndx1[i], compareIndx2[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;
  private Schema outputSchema;
  private int[] compareIndx1;
  private int[] compareIndx2;
  private HashMap<Integer, List<IndexedTuple>> hashTable1;
  private HashMap<Integer, List<IndexedTuple>> hashTable2;
  private TupleBatchBuffer ans;

  /**
   * For Java serialization.
   */
  public LocalJoin() {
  }

  public LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
    hashTable1 = new HashMap<Integer, List<IndexedTuple>>();
    hashTable2 = new HashMap<Integer, List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
  }

  protected void addToAns(final IndexedTuple tuple1, final IndexedTuple tuple2) {
    final int num1 = tuple1.tb.getSchema().numFields();
    final int num2 = tuple2.tb.getSchema().numFields();
    for (int i = 0; i < num1; ++i) {
      ans.put(i, tuple1.tb.getObject(i, tuple1.index));
    }
    for (int i = 0; i < num2; ++i) {
      ans.put(i + num1, tuple2.tb.getObject(i, tuple2.index));
    }
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasNewTuple = false; // might change to EOS instead of hasNext()
      TupleBatch tb = null;
      if ((tb = child1.next()) != null) {
        hasNewTuple = true;
        processChildTB(tb, true);
      }
      // child2
      if ((tb = child2.next()) != null) {
        hasNewTuple = true;
        processChildTB(tb, false);
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

  protected void processChildTB(final TupleBatch tb, final boolean tbFromChild1) {
    List<IndexedTuple> tupleList = null;
    int[] compareIndx2Add = compareIndx1;
    int[] compareIndx2Join = compareIndx2;
    HashMap<Integer, List<IndexedTuple>> hashTable2Add = hashTable1;
    HashMap<Integer, List<IndexedTuple>> hashTable2Join = hashTable2;
    if (!tbFromChild1) {
      compareIndx2Add = compareIndx2;
      compareIndx2Join = compareIndx1;
      hashTable2Add = hashTable2;
      hashTable2Join = hashTable1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) { // outputTuples?
      final IndexedTuple tuple = new IndexedTuple(tb, i);
      final int cntHashCode = tuple.hashCode4Keys(compareIndx2Add);
      tupleList = hashTable2Join.get(cntHashCode);
      if (tupleList != null) {
        for (final IndexedTuple tuple2 : tupleList) {
          if (tuple.joinEquals(tuple2, compareIndx2Add, compareIndx2Join)) {
            if (tbFromChild1) {
              addToAns(tuple, tuple2);
            } else {
              addToAns(tuple2, tuple);
            }
          }
        }
      }
      tupleList = hashTable2Add.get(cntHashCode);
      if (tupleList == null) {
        tupleList = new LinkedList<IndexedTuple>();
        hashTable2Add.put(cntHashCode, tupleList);
      }
      tupleList.add(tuple);
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    child1 = (Operator) in.readObject();
    child2 = (Operator) in.readObject();
    compareIndx1 = (int[]) in.readObject();
    compareIndx2 = (int[]) in.readObject();
    outputSchema = (Schema) in.readObject();
    hashTable1 = new HashMap<Integer, List<IndexedTuple>>();
    hashTable2 = new HashMap<Integer, List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(child1);
    out.writeObject(child2);
    out.writeObject(compareIndx1);
    out.writeObject(compareIndx2);
    out.writeObject(outputSchema);
  }

}
