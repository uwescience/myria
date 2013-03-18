package edu.washington.escience.myriad.operator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

public final class LocalProjectingJoin_RefOnly extends Operator implements Externalizable {

  private class IndexedTuple {
    private final int index;
    private final TupleBatch tb;

    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    public boolean compareField(final IndexedTuple another, final int colIndx1, final int colIndx2) {
      final Type type1 = tb.getSchema().getColumnType(colIndx1);
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
  private int[] answerColumns1;
  private int[] answerColumns2;
  private HashMap<Integer, List<IndexedTuple>> hashTable1;
  private HashMap<Integer, List<IndexedTuple>> hashTable2;
  private TupleBatchBuffer ans;

  /**
   * For Java serialization.
   */
  public LocalProjectingJoin_RefOnly() {
  }

  public LocalProjectingJoin_RefOnly(final Operator child1, final int[] compareIndx1, final int[] answerColumns1,
      final Operator child2, final int[] compareIndx2, final int[] answerColumns2) {
    final List<Type> types = new LinkedList<Type>();
    final List<String> names = new LinkedList<String>();

    for (final int i : answerColumns1) {
      types.add(child1.getSchema().getColumnType(i));
      names.add(child1.getSchema().getColumnName(i));
    }
    for (final int i : answerColumns2) {
      types.add(child2.getSchema().getColumnType(i));
      names.add(child2.getSchema().getColumnName(i));
    }
    outputSchema = new Schema(types, names);
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
    this.answerColumns1 = answerColumns1;
    this.answerColumns2 = answerColumns2;
    hashTable1 = new HashMap<Integer, List<IndexedTuple>>();
    hashTable2 = new HashMap<Integer, List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
  }

  protected void addToAns(final IndexedTuple tuple1, final IndexedTuple tuple2) {
    int curColumn = 0;
    for (final int i : answerColumns1) {
      ans.put(curColumn, tuple1.tb.getObject(i, tuple1.index));
      curColumn++;
    }
    for (final int i : answerColumns2) {
      ans.put(curColumn, tuple2.tb.getObject(i, tuple2.index));
      curColumn++;
    }
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasNewTuple = false; // might change to EOS instead of hasNext()
      TupleBatch tb = null;
      if ((tb = child1.next()) != null) {
        hasNewTuple = true;
        processChild1TB(tb);
      }
      // child2
      if ((tb = child2.next()) != null) {
        hasNewTuple = true;
        processChild2TB(tb);
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
    throw new UnsupportedOperationException("This operator is not implemented.");
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
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  protected void processChild1TB(final TupleBatch tbFromChild1) {
    for (int i = 0; i < tbFromChild1.numTuples(); ++i) { // outputTuples?
      final IndexedTuple tuple1 = new IndexedTuple(tbFromChild1, i);
      final int cntHashCode = tuple1.hashCode4Keys(compareIndx1);

      if (hashTable2.get(cntHashCode) != null) {
        final List<IndexedTuple> tupleList = hashTable2.get(cntHashCode);
        for (int j = 0; j < tupleList.size(); ++j) {
          final IndexedTuple tuple2 = tupleList.get(j);
          if (tuple1.joinEquals(tuple2, compareIndx1, compareIndx2)) {
            addToAns(tuple1, tuple2);
          }
        }
      }

      if (hashTable1.get(cntHashCode) == null) {
        hashTable1.put(cntHashCode, new ArrayList<IndexedTuple>());
      }
      final List<IndexedTuple> tupleList = hashTable1.get(cntHashCode);
      tupleList.add(tuple1);
    }
  }

  protected void processChild2TB(final TupleBatch tbFromChild2) {
    for (int i = 0; i < tbFromChild2.numTuples(); ++i) { // outputTuples?
      final IndexedTuple tuple2 = new IndexedTuple(tbFromChild2, i);
      final int cntHashCode = tuple2.hashCode4Keys(compareIndx2);

      if (hashTable1.get(cntHashCode) != null) {
        final List<IndexedTuple> tupleList = hashTable1.get(cntHashCode);
        for (int j = 0; j < tupleList.size(); ++j) {
          final IndexedTuple tuple1 = tupleList.get(j);
          if (tuple2.joinEquals(tuple1, compareIndx2, compareIndx1)) {
            addToAns(tuple1, tuple2);
          }
        }
      }

      if (hashTable2.get(cntHashCode) == null) {
        hashTable2.put(cntHashCode, new ArrayList<IndexedTuple>());
      }
      final List<IndexedTuple> tupleList = hashTable2.get(cntHashCode);
      tupleList.add(tuple2);
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    child1 = (Operator) in.readObject();
    child2 = (Operator) in.readObject();
    compareIndx1 = (int[]) in.readObject();
    compareIndx2 = (int[]) in.readObject();
    answerColumns1 = (int[]) in.readObject();
    answerColumns2 = (int[]) in.readObject();
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
    out.writeObject(answerColumns1);
    out.writeObject(answerColumns2);
    out.writeObject(outputSchema);
  }

}
