package edu.washington.escience.myriad.operator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
  private HashMap<Integer, List<IndexedTuple>> hashTable1;
  private HashMap<Integer, List<IndexedTuple>> hashTable2;
  private TupleBatchBuffer ans;
  /** Which columns in the left child are to be output. */
  private int[] answerColumns1;
  /** Which columns in the right child are to be output. */
  private int[] answerColumns2;

  /**
   * For Java serialization.
   */
  public LocalJoin() {
  }

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public LocalJoin(final Operator child1, final Operator child2, final int[] compareIndx1, final int[] compareIndx2) {
    this(Schema.merge(child1.getSchema(), child2.getSchema()), child1, child2, compareIndx1, compareIndx2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalJoin(final Operator child1, final Operator child2, final int[] compareIndx1, final int[] compareIndx2,
      final int[] answerColumns1, final int[] answerColumns2) {
    this(mergeFilter(child1.getSchema(), child2.getSchema(), answerColumns1, answerColumns2), child1, child2,
        compareIndx1, compareIndx2, answerColumns1, answerColumns2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param outputSchema the Schema of the output table.
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    Objects.requireNonNull(outputSchema);
    Objects.requireNonNull(child1);
    Objects.requireNonNull(child2);
    Objects.requireNonNull(compareIndx1);
    Objects.requireNonNull(compareIndx2);
    Objects.requireNonNull(answerColumns1);
    Objects.requireNonNull(answerColumns2);

    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length,
        "Must compare the same number of columns from both children");
    Preconditions.checkArgument(outputSchema.numColumns() == answerColumns1.length + answerColumns2.length,
        "Number of columns in provided schema must match the number of output columns");

    /* TODO FIX the below checks. */
    // Preconditions.checkArgument(outputSchema.getColumnTypes().subList(0, child1.getSchema().numColumns()).equals(
    // child1.getSchema().getColumnTypes()),
    // "Types of columns in provided schema must match the concatenation of children schema.");
    // Preconditions.checkArgument(outputSchema.getColumnTypes().subList(child1.getSchema().numColumns(),
    // outputSchema.numColumns()).equals(child2.getSchema().getColumnTypes()),
    // "Types of columns in provided schema must match the concatenation of children schema.");

    this.outputSchema = outputSchema;
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
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param outputSchema the Schema of the output table.
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
    this(outputSchema, child1, child2, compareIndx1, compareIndx2, range(child1.getSchema().numColumns()), range(child2
        .getSchema().numColumns()));
  }

  /**
   * Helper function that generates an array of the numbers 0..max-1.
   * 
   * @param max the size of the array.
   * @return an array of the numbers 0..max-1.
   */
  private static int[] range(final int max) {
    int[] ret = new int[max];
    for (int i = 0; i < max; ++i) {
      ret[i] = i;
    }
    return ret;
  }

  /**
   * Helper function to generate the proper output schema merging two parts of two schemas.
   * 
   * @param schema1 the left schema.
   * @param schema2 the right schema.
   * @param answerColumns1 the selected columns of the left schema.
   * @param answerColumns2 the selected columns of the right schema.
   * @return a schema that contains the chosen columns of the left and right schema.
   */
  private static Schema mergeFilter(final Schema schema1, final Schema schema2, final int[] answerColumns1,
      final int[] answerColumns2) {
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i : answerColumns1) {
      types.add(schema1.getColumnType(i));
      names.add(schema1.getColumnName(i));
    }
    for (int i : answerColumns2) {
      types.add(schema2.getColumnType(i));
      names.add(schema2.getColumnName(i));
    }

    return new Schema(types, names);
  }

  protected void addToAns(final IndexedTuple tuple1, final IndexedTuple tuple2) {
    int count = 0;
    for (int i : answerColumns1) {
      ans.put(count, tuple1.tb.getObject(i, tuple1.index));
      count++;
    }
    for (int i : answerColumns2) {
      ans.put(count, tuple2.tb.getObject(i, tuple2.index));
      count++;
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
    answerColumns1 = (int[]) in.readObject();
    answerColumns2 = (int[]) in.readObject();
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
    out.writeObject(answerColumns1);
    out.writeObject(answerColumns2);
  }

}
