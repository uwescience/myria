package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;

public class MultiwayJoin extends NAryOperator {

  /**
   * required.
   */
  private static final long serialVersionUID = 1L;

  /**
   * stores mapping from joined fields to the fields of child tables. This also implies variable ordering. E.g.
   * joinFieldsMapping[0]: List of joined fields in child tables and will be the firstly iterated.
   * 
   */
  private final List<List<JoinField>> joinFieldMapping;

  /**
   * stores the index of a join field in the local variable ordering.
   */
  private final List<List<JoinFieldOrder>> joinFieldLocalOrder;

  /**
   * stores the index of i-th local variable to the field index.
   */
  private final List<List<JoinFieldOrder>> localOrderedJoinField;

  /**
   * stores mapping from output fields to child table's fields.
   */
  private final List<JoinField> outputFieldMapping;

  /**
   * output column names.
   */
  private final ImmutableList<String> outputColumnNames;

  /**
   * The buffer holding the valid tuples from children.
   */
  private transient TupleBuffer[] tables;

  /**
   * An internal state to record how many children have EOSed.
   */
  private transient int numberOfEOSChild = 0;

  /**
   * An internal state to represent whether join has finished.
   */
  private transient boolean joinFinished = false;

  /**
   * Iterators on child tables.
   */
  private transient TableIterator[] iterators;

  /**
   * current join field (index of {@link joinFieldMapping} ).
   */
  private transient int currentDepth;

  /**
   * current iterator index in joinFieldMapping[currentDepth].
   */
  private transient int currentIteratorIndex;

  /**
   * answer buffer.
   */
  private transient TupleBatchBuffer ansTBB;

  /**
   * Pointer to a cell in a table.
   * 
   */
  private final class CellPointer {
    private final int tableIndex;

    public int getTableIndex() {
      return tableIndex;
    }

    public int getFieldIndex() {
      return fieldIndex;
    }

    private final int fieldIndex;
    private int row;

    public int getRow() {
      return row;
    }

    public void setRow(int row) {
      Preconditions.checkArgument(row >= 0 && row <= tables[tableIndex].numTuples());
      this.row = row;
    }

    public CellPointer(int tableIndex, int fieldIndex, int row) {
      Preconditions.checkElementIndex(tableIndex, tables.length);
      Preconditions.checkElementIndex(fieldIndex, tables[tableIndex].numColumns());
      Preconditions.checkArgument(row >= 0 && row <= tables[tableIndex].numTuples());
      this.tableIndex = tableIndex;
      this.fieldIndex = fieldIndex;
      this.row = row;
    }

    public CellPointer(CellPointer cp) {
      this(cp.getTableIndex(), cp.getFieldIndex(), cp.getRow());
    }

    public boolean atEnd() {
      if (row == tables[tableIndex].numTuples()) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * 
   * Indicate a field in a child table
   */
  private final class JoinField {
    /**
     * index of table containing this field in children
     */
    private final int tableIndex;

    /**
     * index of this field in its owner table
     */
    private final int fieldIndex;

    public JoinField(int tableIndex, int fieldIndex) {
      this.tableIndex = tableIndex;
      this.fieldIndex = fieldIndex;
    }
  }

  /**
   * Iterator of table, which implements a Trie like interface.
   * 
   */
  private final class TableIterator {
    private final int tableIndex;
    private int currentField = -1;
    private final int[] rowIndices;

    public int getRow() {
      return rowIndices[currentField];
    }

    public void setRow(int currentRow) {
      rowIndices[currentField] = currentRow;
    }

    public int getCurrentField() {
      return currentField;
    }

    private void setCurrentField(int currentField) {
      this.currentField = currentField;
    }

    /**
     * Return to the parent key at the previous depth
     */
    public void up() {
      final int localOrder = joinFieldLocalOrder.get(tableIndex).get(currentField).order;
      if (localOrder == 0) {
        return;
      } else {
        currentField = localOrderedJoinField.get(tableIndex).get(localOrder - 1).fieldIndex;
      }
    }

    /**
     * Proceed to the first key at the next depth
     */
    public void open() {
      final int lastField = currentField;
      final int localOrder = joinFieldLocalOrder.get(tableIndex).get(currentField).order;
      if (localOrder == joinFieldLocalOrder.get(tableIndex).size() - 1) {
        return;
      } else {
        currentField = localOrderedJoinField.get(tableIndex).get(localOrder + 1).fieldIndex;
        rowIndices[currentField] = ranges[lastField].getMinRow();
      }
    }

    /**
     * go to the next row
     * 
     * @return whether the iterator reaches the end.
     */
    public boolean next() {
      rowIndices[currentField]++;
      if (getRow() >= ranges[currentField].getMaxRow()) {
        return true;
      } else {
        return false;
      }
    }

    /**
     * proceed to the next value of current field
     */
    public void nextValue() {
      rowIndices[currentField] = ranges[currentField].getMaxRow();
    }

    private final IteratorRange[] ranges;

    public TableIterator(final int tableIndex) {
      Preconditions.checkPositionIndex(tableIndex, getChildren().length);
      this.tableIndex = tableIndex;

      /* initiate ranges */
      ranges = new IteratorRange[tables[tableIndex].numColumns()];
      Arrays.fill(ranges, new IteratorRange(-1, -1));

      /* initiate rowIndices */
      rowIndices = new int[getChildren().length];
      Arrays.fill(rowIndices, -1);
    }

    private final class IteratorRange {
      /**
       * minRow is reachable.
       */
      private int minRow;

      public int getMinRow() {
        return minRow;
      }

      public void setMinRow(int minRow) {
        this.minRow = minRow;
      }

      public int getMaxRow() {
        return maxRow;
      }

      public void setMaxRow(int maxRow) {
        this.maxRow = maxRow;
      }

      public boolean contains(int row) {
        if (row < maxRow && row >= minRow) {
          return true;
        } else {
          return false;
        }
      }

      /**
       * maxRow is unreachable.
       */
      private int maxRow;

      public IteratorRange(int minRow, int maxRow) {
        this.minRow = minRow;
        this.maxRow = maxRow;
      }
    }

    /**
     * @return whether this iterator reaches the end of the table.
     */
    public boolean atEndOfTable() {
      return getRow() >= tables[tableIndex].numTuples();
    }
  }

  /**
   * Comparator class for sorting iterators.
   */
  private class JoinIteratorCompare implements Comparator<JoinField> {
    @Override
    public int compare(JoinField o1, JoinField o2) {
      return tables[o1.tableIndex].compare(o1.fieldIndex, iterators[o1.tableIndex].getRow(), tables[o2.tableIndex],
          o2.fieldIndex, iterators[o2.tableIndex].getRow());
    }
  }

  /**
   * record a field in a table and its join order
   * 
   */
  private final class JoinFieldOrder {
    /**
     * join order of this field
     */
    private int order;
    /**
     * field index of this field.
     */
    private final int fieldIndex;

    /**
     * @return order
     */
    private int getOrder() {
      return order;
    }

    /**
     * @param order set order.
     */
    private void setOrder(int order) {
      this.order = order;
    }

    /**
     * @return field index in a table..
     */
    private int getFieldIndex() {
      return fieldIndex;
    }

    public JoinFieldOrder(int order, final int fieldIndex) {
      this.order = order;
      this.fieldIndex = fieldIndex;
    }

    @Override
    public String toString() {
      return fieldIndex + ":" + order;
    }
  }

  /**
   * @param children list of child operators
   * @param joinFieldMapping mapping of output field to child table field
   * @param outputColumns output column names
   */
  public MultiwayJoin(final Operator[] children, final List<List<List<Integer>>> joinFieldMapping,
      final List<List<Integer>> outputFieldMapping, final List<String> outputColumnNames) {
    if (outputColumnNames != null) {
      Preconditions.checkArgument(joinFieldMapping.size() == outputColumnNames.size(),
          "outputColumns and JoinFieldMapping should have the same cardinality.");
    }
    /* set children */
    setChildren(children);

    /* set join field mapping and reverse field mapping */
    this.joinFieldMapping = new ArrayList<List<JoinField>>();
    joinFieldLocalOrder = new ArrayList<>(children.length);
    for (Operator element : children) {
      List<JoinFieldOrder> localOrder = new ArrayList<>();
      for (int i = 0; i < element.getSchema().numColumns(); ++i) {
        localOrder.add(new JoinFieldOrder(-1, i));
      }
      joinFieldLocalOrder.add(localOrder);
    }
    for (int i = 0; i < joinFieldMapping.size(); ++i) {
      List<JoinField> joinedFieldList = new ArrayList<JoinField>();
      for (int j = 0; j < joinFieldMapping.get(i).size(); ++j) {
        // get table index and field index of each join field
        Preconditions.checkArgument(joinFieldMapping.get(i).get(j).size() == 2);
        int tableIndex = joinFieldMapping.get(i).get(j).get(0);
        int fieldIndex = joinFieldMapping.get(i).get(j).get(1);
        // update joinFieldMapping and reverseJoinFieldMapping
        Preconditions.checkPositionIndex(tableIndex, children.length);
        Preconditions.checkPositionIndex(fieldIndex, children[tableIndex].getSchema().numColumns());
        joinedFieldList.add(new JoinField(tableIndex, fieldIndex));
        joinFieldLocalOrder.get(tableIndex).get(fieldIndex).setOrder(i);
      }
      this.joinFieldMapping.add(joinedFieldList);
    }

    localOrderedJoinField = new ArrayList<>();
    for (int i = 0; i < joinFieldLocalOrder.size(); ++i) {

      List<JoinFieldOrder> orderedJoinField = new ArrayList<>();
      for (JoinFieldOrder localOrder : joinFieldLocalOrder.get(i)) {
        orderedJoinField.add(new JoinFieldOrder(localOrder.getOrder(), localOrder.getFieldIndex()));
      }
      Collections.sort(orderedJoinField, new Comparator<JoinFieldOrder>() {
        @Override
        public int compare(JoinFieldOrder o1, JoinFieldOrder o2) {
          if (o1.getOrder() == -1 && o2.getOrder() == -1) {
            return 0;
          } else if (o1.getOrder() == -1 && o2.getOrder() != -1) {
            return -1;
          } else if (o1.getOrder() != -1 && o2.getOrder() == -1) {
            return 1;
          } else {
            return Integer.compare(o1.getOrder(), o2.getOrder());
          }
        }
      });
      localOrderedJoinField.add(orderedJoinField);
    }

    /* convert the global order to the local order and update the local order back to the joinFieldLocalOrder. */
    for (int i = 0; i < localOrderedJoinField.size(); ++i) {
      List<JoinFieldOrder> orderedJoinField = localOrderedJoinField.get(i);
      for (int j = 0; j < orderedJoinField.size(); j++) {
        JoinFieldOrder joinFieldOrder = orderedJoinField.get(j);
        if (joinFieldOrder.getOrder() != -1) {
          joinFieldOrder.setOrder(j);
          joinFieldLocalOrder.get(i).get(joinFieldOrder.getFieldIndex()).setOrder(j);
        }
      }
    }

    /* for debugging only. */
    System.out.println("localOrderedJoinField: ");
    for (List<JoinFieldOrder> localOrderList : localOrderedJoinField) {
      System.out.println(Arrays.toString(localOrderList.toArray()));
    }

    System.out.println("joinFieldLocalOrder: ");
    for (List<JoinFieldOrder> localOrderList : joinFieldLocalOrder) {
      System.out.println(Arrays.toString(localOrderList.toArray()));
    }
    /* set output field */
    this.outputFieldMapping = new ArrayList<JoinField>();
    for (int i = 0; i < outputFieldMapping.size(); ++i) {
      Preconditions.checkArgument(outputFieldMapping.get(i).size() == 2);
      this.outputFieldMapping.add(new JoinField(outputFieldMapping.get(i).get(0), outputFieldMapping.get(i).get(1)));
    }

    /* set output schema */
    if (outputColumnNames != null) {
      Preconditions.checkArgument(ImmutableSet.copyOf(outputColumnNames).size() == outputColumnNames.size(),
          "duplicate names in output schema. ");
      this.outputColumnNames = ImmutableList.copyOf(outputColumnNames);
    } else {
      this.outputColumnNames = null;
    }

    if (children.length > 0) {
      generateSchema();
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {

    /* draining all the children. */
    Operator[] children = getChildren();
    while (numberOfEOSChild != children.length) {
      int numberOfNoDataChild = 0;
      for (int i = 0; i < children.length; ++i) {
        Operator child = children[i];
        if (!child.eos()) {
          TupleBatch childTB = child.nextReady();
          if (childTB == null) {
            if (child.eos()) {
              numberOfEOSChild++;
            }
            numberOfNoDataChild++;
          } else {
            storeChildTuple(i, childTB);
          }
        } else {
          // if a child is eos, it should be treated as no data child
          numberOfNoDataChild++;
        }
      }
      if (numberOfNoDataChild == children.length && numberOfEOSChild != children.length) {
        return null;
      }
    }

    leapfrog_join();
    TupleBatch nexttb = ansTBB.popAny();
    if (nexttb != null) {
      return nexttb;
    } else if (nexttb == null && joinFinished) {
      setEOS();
      return null;
    } else {
      throw new RuntimeException("incorrect return.");
    }

  }

  @Override
  protected Schema generateSchema() {
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    Operator[] children = getChildren();
    for (int i = 0; i < outputFieldMapping.size(); ++i) {
      types.add(children[outputFieldMapping.get(i).tableIndex].getSchema().getColumnType(
          outputFieldMapping.get(i).fieldIndex));
      names.add(children[outputFieldMapping.get(i).tableIndex].getSchema().getColumnName(
          outputFieldMapping.get(i).fieldIndex));
    }
    if (outputColumnNames != null) {
      return new Schema(types.build(), outputColumnNames);
    } else {
      return new Schema(types, names);
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    generateSchema();
    Operator[] children = getChildren();
    for (int i = 0; i < children.length; ++i) {
      tables[i] = new TupleBuffer(children[i].getSchema());
    }
    iterators = new TableIterator[children.length];
    for (int i = 0; i < children.length; ++i) {
      iterators[i] = new TableIterator(i);
    }
    currentDepth = -1;

  }

  @Override
  protected void cleanup() throws DbException {
    Operator[] children = getChildren();
    for (int i = 0; i < children.length; ++i) {
      tables[i] = null;
    }
    tables = null;
    for (int i = 0; i < iterators.length; ++i) {
      iterators[i] = null;
    }
    iterators = null;
  }

  /**
   * @param childIndex which child to store
   * @param tb incoming tuple
   */
  private void storeChildTuple(int childIndex, TupleBatch tb) {
    List<Column<?>> inputColumns = tb.getDataColumns();
    for (int row = 0; row < tb.numTuples(); ++row) {
      int inColumnRow = tb.getValidIndices().get(row);
      for (int column = 0; column < tb.numColumns(); column++) {
        tables[childIndex].put(column, inputColumns.get(column), inColumnRow);
      }
    }
  }

  /**
   * Start leap-frog join.
   */
  private void leapfrog_init() {

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final int localOrder = joinFieldLocalOrder.get(jf.tableIndex).get(jf.fieldIndex).order;
      final TableIterator it = iterators[jf.tableIndex];
      if (localOrder == 0) {
        /* if the join field is highest ordered, reset the cursor */
        it.ranges[jf.fieldIndex].setMinRow(0);
        it.ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
        it.setCurrentField(jf.fieldIndex);
        it.setRow(0);

      } else {
        /* if the join field is not ordered as the first, reset the cursor to last level */
        final int lastJf = localOrderedJoinField.get(jf.tableIndex).get(localOrder - 1).fieldIndex;
        it.ranges[jf.fieldIndex].setMinRow(it.ranges[lastJf].getMinRow());
        it.ranges[jf.fieldIndex].setMaxRow(it.ranges[lastJf].getMaxRow());
        it.setCurrentField(jf.fieldIndex);
        it.setRow(it.ranges[lastJf].getMinRow());
      }
    }

    Collections.sort(joinFieldMapping.get(currentDepth), new JoinIteratorCompare());
    currentIteratorIndex = 0;
  }

  /**
   * Assuming {@value currentIteratorIndex}th iterator is pointing the max key and (currentIteratorIndex - 1)%k } th
   * iterator is pointing at the least key.
   * 
   * find the next intersection in current join field.
   * 
   * @return at end or not.
   */
  private boolean leapfrog_search() {
    boolean atEnd = false;
    JoinField fieldWithMaxKey =
        joinFieldMapping.get(currentDepth).get((currentIteratorIndex - 1) % joinFieldMapping.get(currentDepth).size());
    CellPointer maxKey =
        new CellPointer(fieldWithMaxKey.tableIndex, fieldWithMaxKey.fieldIndex, iterators[fieldWithMaxKey.tableIndex]
            .getRow());

    while (true) {
      JoinField fieldWithLeastKey = joinFieldMapping.get(currentDepth).get(currentIteratorIndex);
      CellPointer leastKey =
          new CellPointer(fieldWithLeastKey.tableIndex, fieldWithLeastKey.fieldIndex,
              iterators[fieldWithLeastKey.tableIndex].getRow());
      if (cellCompare(leastKey, maxKey) == 0) { // if the value current
        refineRanges();
        break;
      } else {
        atEnd = leapfrog_seek(fieldWithLeastKey, maxKey);
        if (atEnd) {
          break;
        } else {// if leapfrog_seek hasn't reach end, update max key, move to the next table
          maxKey =
              new CellPointer(fieldWithLeastKey.tableIndex, fieldWithLeastKey.fieldIndex,
                  iterators[fieldWithLeastKey.tableIndex].getRow());
          currentIteratorIndex = (currentIteratorIndex + 1) % joinFieldMapping.get(currentDepth).size();
        }
      }
    }
    return atEnd;
  }

  /* TODO: for each join field, find the boundary of current value. */
  public void refineRanges() {

  }

  /**
   * move the iterator to the element which is the first key larger than current max.
   * 
   * @param jf seek on which field of which table.
   * @param target the target value of seeking.
   * @return at end or not.
   */
  private boolean leapfrog_seek(JoinField jf, CellPointer target) {
    int startRow = iterators[jf.tableIndex].getRow();
    int maxRow = iterators[jf.tableIndex].ranges[jf.fieldIndex].getMaxRow() - 1;
    final CellPointer startCursor = new CellPointer(jf.tableIndex, jf.fieldIndex, startRow);
    CellPointer cursor = new CellPointer(startCursor);

    /* short-cuts, return result early. */
    if (cellCompare(startCursor, target) >= 0) {
      return false;
    }
    /* set row number to upper bound */
    cursor.setRow(maxRow);
    if (cellCompare(cursor, target) < 0) {
      return true;
    }

    /* binary search: find the first row whose value is not less than target */
    while (true) {
      cursor.setRow((maxRow + startRow) / 2);
      int compare = cellCompare(cursor, target);
      if (compare >= 0) { // cursor > target
        maxRow = cursor.getRow();
      } else if (compare < 0) { // cursor < target
        startRow = cursor.getRow();
      }

      if (startRow == maxRow - 1) {
        cursor.setRow(maxRow);
        iterators[jf.tableIndex].setRow(maxRow);
        return false;
      }
    }
  }

  /**
   * @param cp1 CellPointer 1
   * @param cp2 CellPointer 2
   * @return whether two rows have the same higher ordered variables
   */
  private boolean compareHigherOrdered(CellPointer cp1, CellPointer cp2) {
    Preconditions.checkArgument(cp1.getTableIndex() == cp2.getTableIndex());
    for (int i = 0; i < joinFieldLocalOrder.get(cp2.getTableIndex()).get(cp2.getFieldIndex()).getOrder(); i++) {
      int fieldIndex = localOrderedJoinField.get(cp2.getTableIndex()).get(i).getFieldIndex();
      if (cellCompare(new CellPointer(cp1.getTableIndex(), fieldIndex, cp1.getRow()), new CellPointer(cp2
          .getTableIndex(), fieldIndex, cp2.getRow())) != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Leapfrog join.
   */
  private void leapfrog_join() {

    /* initiate the join for the first time */
    if (currentDepth == -1) {
      leapfrog_init();
      currentDepth = 0;
    }

    while (ansTBB.numTuples() < TupleBatch.BATCH_SIZE) {
      boolean atEnd = leapfrog_search();
      if (atEnd && currentDepth == 0) {
        /* if the first join variable reaches end, then the join finish. */
        joinFinished = true;
        return;

      } else if (atEnd) {
        /* reach to the end in current depth, go back to last depth */
        join_up();

      } else if (currentDepth == joinFieldMapping.size() - 1) {
        /* exhaust all output with current join key */
        exhaustOutput(0);

        /* move to the next value */
        iterators[joinFieldMapping.get(currentDepth).get(currentIteratorIndex).tableIndex].nextValue();
      } else {
        /* go to the next join variable. */
        join_open();

      }
    }
  }

  /**
   * advance to the next join variable.
   */
  private void join_open() {
    currentDepth++;
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.tableIndex].open();

      if (currentDepth == 0) {
        // if that is initial open.
        iterators[jf.tableIndex].ranges[jf.fieldIndex].setMinRow(0);
        iterators[jf.tableIndex].ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
      }
    }
    leapfrog_init();
  }

  /**
   * backtrack to previous join variable.
   */
  private void join_up() {
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.tableIndex].up();
    }
    currentDepth--;
  }

  /**
   * Recursively output all result tuples sharing the same join key(s).
   * 
   * @param startPositions start position of iterators.
   */
  private void exhaustOutput(final int index) {
    JoinField currentJF = joinFieldMapping.get(currentDepth).get(index);
    int currentRow = iterators[currentJF.tableIndex].ranges[index].minRow;
    for (; currentRow < iterators[currentJF.tableIndex].ranges[index].maxRow; currentRow++) {
      iterators[currentJF.tableIndex].setRow(currentRow);
      if (index == joinFieldMapping.get(currentDepth).size() - 1) {
        addToAns();
      } else {
        exhaustOutput(index + 1);
      }
    }
  }

  /**
   * add result to answer.
   */
  private void addToAns() {
    for (int i = 0; i < outputFieldMapping.size(); ++i) {
      ansTBB.put(tables[outputFieldMapping.get(i).tableIndex], outputFieldMapping.get(i).fieldIndex,
          iterators[outputFieldMapping.get(i).tableIndex].getRow(), i);
    }
  }

  /**
   * @param cp1 CellPointer 1
   * @param cp2 CellPointer 2
   * @return result of comparison
   */
  private int cellCompare(CellPointer cp1, CellPointer cp2) {
    return tables[cp1.tableIndex].compare(cp1.getFieldIndex(), cp1.getRow(), tables[cp2.getTableIndex()], cp2
        .getFieldIndex(), cp2.getRow());

  }

}
