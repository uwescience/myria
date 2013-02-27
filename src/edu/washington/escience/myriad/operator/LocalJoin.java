package edu.washington.escience.myriad.operator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

public final class LocalJoin extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;

  private final Schema outputSchema;
  private final int[] compareIndx1;
  private final int[] compareIndx2;
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  private transient TupleBatchBuffer hashTable1;
  private transient TupleBatchBuffer hashTable2;
  private transient TupleBatchBuffer ans;
  /** Which columns in the left child are to be output. */
  private final int[] answerColumns1;
  /** Which columns in the right child are to be output. */
  private final int[] answerColumns2;

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

  protected void addToAns(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int index,
      final boolean fromChild1) {
    if (fromChild1) {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, cntTuple.get(answerColumns1[i]));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, hashTable.get(answerColumns2[i], index));
      }
    } else {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, hashTable.get(answerColumns1[i], index));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, cntTuple.get(answerColumns2[i]));
      }
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

  private boolean compareTuple(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int index,
      final int[] compareIndx1, final int[] compareIndx2) {
    if (compareIndx1.length != compareIndx2.length) {
      return false;
    }
    for (int i = 0; i < compareIndx1.length; ++i) {
      if (!cntTuple.get(compareIndx1[i]).equals(hashTable.get(compareIndx2[i], index))) {
        return false;
      }
    }
    return true;
  }

  protected void processChildTB(final TupleBatch tb, final TupleBatchBuffer hashTable1,
      final TupleBatchBuffer hashTable2, final HashMap<Integer, List<Integer>> hashTable1Indices,
      final HashMap<Integer, List<Integer>> hashTable2Indices, final int[] compareIndx1, final int[] compareIndx2,
      final boolean fromChild1) {

    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = hashTable1.numTuples();
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
        hashTable1.put(j, cntTuple.get(j));
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
    hashTable1 = new TupleBatchBuffer(child1.getSchema());
    hashTable2 = new TupleBatchBuffer(child2.getSchema());
    ans = new TupleBatchBuffer(getSchema());
  }

  private void writeObject(final ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

}
