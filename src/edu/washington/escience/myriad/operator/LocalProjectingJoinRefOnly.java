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

/**
 * Join and project.
 * */
public final class LocalProjectingJoinRefOnly extends Operator implements Externalizable {

  /**
   * Pointer data structure for pointing to a tuple in a TupleBatch.
   * */
  private class IndexedTuple {
    /**
     * The row index.
     * */
    private final int index;
    /**
     * The source data TB.
     * */
    private final TupleBatch tb;

    /**
     * @param tb the source data TB.
     * @param index the row index.
     * */
    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    /**
     * compare the equality of a column of the indexed tuple and another tuple.
     * 
     * @return true if equal.
     * @param another another indexed tuple
     * @param colIndx1 column index of this indexed tuple
     * @param colIndx2 column index of another indexed tuple
     * */
    public boolean columnEquals(final IndexedTuple another, final int colIndx1, final int colIndx2) {
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
        case DATETIME_TYPE:
          return tb.getDateTime(colIndx1, rowIndx1).equals(another.tb.getDateTime(colIndx2, rowIndx2));
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
        if (!columnEquals(another, i, i)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return tb.hashCode(index);
    }

    /**
     * compute a hash code for all the columns indexed by colIndx.
     * 
     * @param colIndx the columns to compute hash code.
     * @return the hash code of the columns
     * */
    public int hashCode4Keys(final int[] colIndx) {
      return tb.hashCode(index, colIndx);
    }

    /**
     * @return if this index tuple can be equi-joined with another index tuple
     * @param o another indexed tuple
     * @param compareIndx1 column indices of this indexed tuple to compare with
     * @param compareIndx2 column indices of another indexed tuple to compare with
     * */
    public boolean joinEquals(final Object o, final int[] compareIndx1, final int[] compareIndx2) {
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      if (compareIndx1.length != compareIndx2.length) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      for (int i = 0; i < compareIndx1.length; ++i) {
        if (!columnEquals(another, compareIndx1[i], compareIndx2[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The two children.
   * */
  private Operator child1, child2;

  /**
   * The result schema.
   * */
  private Schema outputSchema;

  /**
   * Comparing column indices of child1.
   * */
  private int[] compareIndx1;
  /**
   * Comparing column indices of child2.
   * */
  private int[] compareIndx2;

  /**
   * Hash table for child1 tuples. { hash code -> list of indexed tuples}.
   * */
  private HashMap<Integer, List<IndexedTuple>> hashTable1;

  /**
   * Hash table for child2 tuples. { hash code -> list of indexed tuples}.
   * */
  private HashMap<Integer, List<IndexedTuple>> hashTable2;
  /**
   * Buffer holding the result.
   * */
  private TupleBatchBuffer ans;

  /**
   * Projecting columns for child1.
   * */
  private int[] answerColumns1;
  /**
   * Projecting columns for child2.
   * */
  private int[] answerColumns2;

  /**
   * For Java serialization.
   */
  public LocalProjectingJoinRefOnly() {
  }

  /**
   * @param child1 see field.
   * @param child2 see field.
   * @param compareIndx1 comparing column indices for child1
   * @param compareIndx2 comparing column indices for child2
   * @param answerColumns1 see field.
   * @param answerColumns2 see field.
   * */
  public LocalProjectingJoinRefOnly(final Operator child1, final int[] compareIndx1, final int[] answerColumns1,
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

  /**
   * @param tuple1 join source tuple 1
   * @param tuple2 join source tuple 2
   * */
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

  /**
   * @param tbFromChild1 the source tb for processing.
   * */
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

  /**
   * @param tbFromChild2 the source tb for processing.
   * */
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
