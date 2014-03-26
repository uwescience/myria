package edu.washington.escience.myria.operator;

import java.io.Serializable;
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
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.MutableTupleBuffer;

/**
 * 
 * This operator implement Leap-Frog join algorithm (http://arxiv.org/abs/1210.0481), which takes multiple relations and
 * arbitrary join structure as input.
 * 
 * It takes pre-sorted relations as input. The variable ordering must be implied at joinFieldMapping.
 * 
 * @author chushumo
 * 
 */
public class LeapFrogJoin extends NAryOperator {

  /**
   * required.
   */
  private static final long serialVersionUID = 1L;

  /**
   * {@code joinFieldMappingInit[i]} represents the list of JoinFiled of i-th join variable.
   */
  private final int[][][] joinFieldMappingInit;

  /**
   * {@code outputFieldMappingInit[i]} represents the join field that i-th output column maps to.
   */
  private final int[][] outputFieldMappingInit;

  /**
   * {@code joinFieldMapping[i]} is the list of JoinFields of i-th join variable.
   */
  private transient List<List<JoinField>> joinFieldMapping;

  /**
   * {@code joinFieldLocalOrder[i][j]} stores join field order of j-th field of i-th child's table.
   */
  private transient List<List<JoinFieldOrder>> joinFieldLocalOrder;

  /**
   * {@code joinFieldGlobalOrder[i][j]} stores global order of j-th field of i-th child's table (-1 means not a joining
   * field).
   */
  private transient List<List<JoinFieldOrder>> joinFieldGlobalOrder;

  /**
   * {@code localOrderedJoinField[i][j]} stores the join field (locally) ordered j of i-th child's table.
   */
  private transient List<List<JoinField>> localOrderedJoinField;

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
  private transient MutableTupleBuffer[] tables;

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
    /**
     * Table index of this CellPointer.
     */
    private final int tableIndex;

    /**
     * @return Table index of this CellPointer
     */
    public int getTableIndex() {
      return tableIndex;
    }

    /**
     * @return Field index of this CellPointer
     */
    public int getFieldIndex() {
      return fieldIndex;
    }

    /**
     * Field index of this CellPointer.
     */
    private final int fieldIndex;

    /**
     * row number.
     */
    private int row;

    /**
     * @return row number
     */
    public int getRow() {
      return row;
    }

    /**
     * @param row row number to set
     */
    public void setRow(final int row) {
      Preconditions.checkElementIndex(row, tables[tableIndex].numTuples(), "row out of bound when setting row");
      this.row = row;
    }

    /**
     * @param tableIndex the table index of this CellPointer
     * @param fieldIndex the field index of this CellPointer
     * @param row row number
     */
    public CellPointer(final int tableIndex, final int fieldIndex, final int row) {
      Preconditions.checkElementIndex(tableIndex, tables.length, "tableIndex exceeds legal range.");
      Preconditions.checkElementIndex(fieldIndex, tables[tableIndex].numColumns(), "fieldIndex exceeds legal range.");
      Preconditions.checkState(row >= 0 && row <= tables[tableIndex].numTuples(), "row number exceeds legal range.");
      this.tableIndex = tableIndex;
      this.fieldIndex = fieldIndex;
      this.row = row;
    }

    /**
     * @param cp the CellPointer to copy from
     */
    public CellPointer(final CellPointer cp) {
      this(cp.getTableIndex(), cp.getFieldIndex(), cp.getRow());
    }

    @Override
    public String toString() {
      return "t:" + tableIndex + " f:" + fieldIndex + " r:" + row;
    }
  }

  /**
   * 
   * Indicate a field in a child table.
   */
  private final class JoinField implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * index of table containing this field in children.
     */
    private final int tableIndex;

    /**
     * index of this field in its owner table.
     */
    private final int fieldIndex;

    /**
     * Constructor.
     * 
     * @param tableIndex table index.
     * @param fieldIndex join field index in a table.
     */
    public JoinField(final int tableIndex, final int fieldIndex) {
      this.tableIndex = tableIndex;
      this.fieldIndex = fieldIndex;
    }

    @Override
    public String toString() {
      return tableIndex + ":" + fieldIndex;
    }

  }

  /**
   * Iterator of table, which implements a Trie like interface.
   * 
   */
  private class TableIterator {
    /**
     * table index of this iterator.
     */
    private final int tableIndex;

    /**
     * current field.
     */
    private int currentField = -1;

    /**
     * iterator positions on different fields.
     */
    private final int[] rowIndices;

    /**
     * @return iterator position on current field.
     */
    public int getRowOfCurrentField() {
      return rowIndices[currentField];
    }

    /**
     * @param field field index.
     * @return iterator position on given field.
     */
    public int getRow(final int field) {
      Preconditions.checkElementIndex(field, rowIndices.length, "field index cannot exceed number of columns.");
      return rowIndices[field];
    }

    /**
     * 
     * Set iterator position on current field.
     * 
     * @param row the row number.
     */
    public void setRowOfCurrentField(final int row) {
      rowIndices[currentField] = row;
    }

    /**
     * Set iterator position on given field.
     * 
     * @param field the field index.
     * @param row row number.
     */
    public void setRow(final int field, final int row) {
      Preconditions.checkElementIndex(field, rowIndices.length, "field index cannot exceed number of columns.");
      rowIndices[field] = row;
    }

    /**
     * @param currentField current field.
     */
    private void setCurrentField(final int currentField) {
      this.currentField = currentField;
    }

    /**
     * proceed to the next value of current field.
     */
    public void nextValue() {
      rowIndices[currentField] = ranges[currentField].getMaxRow();
    }

    /**
     * the ranges of different fields.
     */
    private final IteratorRange[] ranges;

    /**
     * @param tableIndex table index.
     */
    public TableIterator(final int tableIndex) {
      Preconditions.checkPositionIndex(tableIndex, getChildren().length,
          "table index cannot exceed number of children.");
      this.tableIndex = tableIndex;

      /* initiate ranges */
      ranges = new IteratorRange[tables[tableIndex].numColumns()];
      for (int i = 0; i < tables[tableIndex].numColumns(); ++i) {
        ranges[i] = new IteratorRange(-1, -1);
      }

      /* initiate rowIndices */
      rowIndices = new int[getChildren().length];
      Arrays.fill(rowIndices, -1);
    }

    /**
     * @return the table index of this iterator.
     */
    @SuppressWarnings("unused")
    public int getTableIndex() {
      return tableIndex;
    }

    /**
     * The legal range of an iterator on a field. This is to simulate the trie data structure.
     * 
     * @author chushumo
     * 
     */
    private class IteratorRange {
      /**
       * minRow is reachable.
       */
      private int minRow;

      /**
       * @return minimal row.
       */
      public int getMinRow() {
        return minRow;
      }

      /**
       * @param minRow minimal row.
       */
      public void setMinRow(final int minRow) {
        this.minRow = minRow;
      }

      /**
       * @return maximal row.
       */
      public int getMaxRow() {
        return maxRow;
      }

      /**
       * @param maxRow maximal row.
       */
      public void setMaxRow(final int maxRow) {
        this.maxRow = maxRow;
      }

      /**
       * maxRow is unreachable.
       */
      private int maxRow;

      /**
       * @param minRow minimal row.
       * @param maxRow maximal row.
       */
      public IteratorRange(final int minRow, final int maxRow) {
        this.minRow = minRow;
        this.maxRow = maxRow;
      }
    }

  }

  /**
   * Comparator class for sorting iterators.
   */
  private class JoinIteratorCompare implements Comparator<JoinField> {
    @Override
    public int compare(final JoinField o1, final JoinField o2) {
      return tables[o1.tableIndex].compare(o1.fieldIndex, iterators[o1.tableIndex].getRowOfCurrentField(),
          tables[o2.tableIndex], o2.fieldIndex, iterators[o2.tableIndex].getRowOfCurrentField());
    }
  }

  /**
   * record a field in a table and its join order.
   * 
   */
  private final class JoinFieldOrder implements Serializable {
    /**
     * required.
     */
    private static final long serialVersionUID = 1L;
    /**
     * join order of this field.
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
    private void setOrder(final int order) {
      this.order = order;
    }

    /**
     * @return field index in a table..
     */
    private int getFieldIndex() {
      return fieldIndex;
    }

    /**
     * @param order order of this join field.
     * @param fieldIndex index of this join filed.
     */
    public JoinFieldOrder(final int order, final int fieldIndex) {
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
   * @param joinFieldMapping mapping of join field to child table field
   * @param outputFieldMapping mapping of output field to child table field
   * @param outputColumnNames output column names
   */
  public LeapFrogJoin(final Operator[] children, final int[][][] joinFieldMapping, final int[][] outputFieldMapping,
      final List<String> outputColumnNames) {
    if (outputColumnNames != null) {
      Preconditions.checkArgument(outputFieldMapping.length == outputColumnNames.size(),
          "outputColumns and outputFieldMapping should have the same cardinality.");
    }
    /* set children */
    setChildren(children);
    joinFieldMappingInit = joinFieldMapping;
    outputFieldMappingInit = outputFieldMapping;

    /* set output schema */
    if (outputColumnNames != null) {
      Preconditions.checkArgument(ImmutableSet.copyOf(outputColumnNames).size() == outputColumnNames.size(),
          "duplicate names in output schema. ");
      this.outputColumnNames = ImmutableList.copyOf(outputColumnNames);
    } else {
      this.outputColumnNames = null;
    }

    /* set output field */
    this.outputFieldMapping = new ArrayList<JoinField>();
    for (int[] element : outputFieldMappingInit) {
      Preconditions.checkArgument(element.length == 2,
          "An array representing join field must be at length of 2. ([tableIndex,fieldIndex])");
      this.outputFieldMapping.add(new JoinField(element[0], element[1]));
    }

  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    /* drain all the children first. */
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
    /* handle the case that one of input tables is empty. */
    if (currentDepth == -1) {
      for (MutableTupleBuffer table : tables) {
        if (table.numTuples() == 0) {
          joinFinished = true;
          checkEOSAndEOI();
          return null;
        }
      }
    }
    /* do the join, pop if there is ready tb. */
    if (!joinFinished) {
      leapfrogJoin();
    }
    TupleBatch nexttb = ansTBB.popAny();

    if (nexttb != null) {
      return nexttb;
    } else if (joinFinished) {
      checkEOSAndEOI();
      return null;
    } else {
      throw new RuntimeException("incorrect return.");
    }
  }

  @Override
  public void checkEOSAndEOI() {
    if (numberOfEOSChild == getChildren().length && ansTBB.numTuples() == 0) {
      setEOS();
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

    Operator[] children = getChildren();

    /* initiate join field mapping and field local order */
    joinFieldMapping = new ArrayList<List<JoinField>>();
    joinFieldLocalOrder = new ArrayList<>(children.length);
    joinFieldGlobalOrder = new ArrayList<>(children.length);
    for (Operator element : children) {
      List<JoinFieldOrder> localOrder = new ArrayList<>();
      List<JoinFieldOrder> globalOrder = new ArrayList<>();
      for (int i = 0; i < element.getSchema().numColumns(); ++i) {
        localOrder.add(new JoinFieldOrder(-1, i));
        globalOrder.add(new JoinFieldOrder(-1, i));
      }
      joinFieldLocalOrder.add(localOrder);
      joinFieldGlobalOrder.add(globalOrder);
    }

    /* set join field mapping and field local order */
    for (int i = 0; i < joinFieldMappingInit.length; ++i) {
      List<JoinField> joinedFieldList = new ArrayList<JoinField>();
      for (int j = 0; j < joinFieldMappingInit[i].length; ++j) {
        // get table index and field index of each join field
        Preconditions.checkArgument(joinFieldMappingInit[i][j].length == 2,
            "the inner arrary of JoinFieldMapping must have the length of 2");
        int tableIndex = joinFieldMappingInit[i][j][0];
        int fieldIndex = joinFieldMappingInit[i][j][1];
        // update joinFieldMapping and reverseJoinFieldMapping
        Preconditions.checkPositionIndex(tableIndex, children.length,
            "table index cannot exceed the number of children.");
        Preconditions.checkPositionIndex(fieldIndex, children[tableIndex].getSchema().numColumns(),
            "filed index cannot exceed the number of columns.");
        joinedFieldList.add(new JoinField(tableIndex, fieldIndex));
        joinFieldLocalOrder.get(tableIndex).get(fieldIndex).setOrder(i);
        joinFieldGlobalOrder.get(tableIndex).get(fieldIndex).setOrder(i);
      }
      joinFieldMapping.add(joinedFieldList);
    }

    localOrderedJoinField = new ArrayList<>();
    for (int i = 0; i < joinFieldLocalOrder.size(); ++i) {

      List<JoinField> jfl = new ArrayList<>();
      List<JoinFieldOrder> orderedJoinFieldOrder = new ArrayList<>();
      for (JoinFieldOrder localOrder : joinFieldLocalOrder.get(i)) {
        orderedJoinFieldOrder.add(new JoinFieldOrder(localOrder.getOrder(), localOrder.getFieldIndex()));
      }

      Collections.sort(orderedJoinFieldOrder, new Comparator<JoinFieldOrder>() {
        @Override
        public int compare(final JoinFieldOrder o1, final JoinFieldOrder o2) {
          if (o1.getOrder() == -1 && o2.getOrder() == -1) {
            return 0;
          } else if (o1.getOrder() == -1 && o2.getOrder() != -1) {
            return 1;
          } else if (o1.getOrder() != -1 && o2.getOrder() == -1) {
            return -1;
          } else {
            return Integer.compare(o1.getOrder(), o2.getOrder());
          }
        }
      });

      for (int j = 0; j < orderedJoinFieldOrder.size(); j++) {
        jfl.add(new JoinField(i, orderedJoinFieldOrder.get(j).getFieldIndex()));
      }
      localOrderedJoinField.add(jfl);
    }

    /* convert the global order to the local order and update the local order back to the joinFieldLocalOrder. */
    for (int i = 0; i < localOrderedJoinField.size(); ++i) {
      List<JoinField> orderedJoinField = localOrderedJoinField.get(i);
      for (int j = 0; j < orderedJoinField.size(); j++) {
        JoinField jf = orderedJoinField.get(j);
        joinFieldLocalOrder.get(i).get(jf.fieldIndex).setOrder(j);
      }
    }

    /* Initiate hash tables */

    tables = new MutableTupleBuffer[children.length];
    for (int i = 0; i < children.length; ++i) {
      tables[i] = new MutableTupleBuffer(children[i].getSchema());
    }
    /* Initiate iterators */
    iterators = new TableIterator[children.length];
    for (int i = 0; i < children.length; ++i) {
      iterators[i] = new TableIterator(i);
    }
    currentDepth = -1;

    ansTBB = new TupleBatchBuffer(getSchema());

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
    ansTBB = null;
  }

  /**
   * @param childIndex which child to store
   * @param tb incoming tuple
   */
  private void storeChildTuple(final int childIndex, final TupleBatch tb) {
    List<Column<?>> inputColumns = tb.getDataColumns();
    for (int row = 0; row < tb.numTuples(); ++row) {
      for (int column = 0; column < tb.numColumns(); column++) {
        tables[childIndex].put(column, inputColumns.get(column), row);
      }
    }
  }

  /**
   * init/restart leap-frog join.
   */
  private void leapfrogInit() {

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final int localOrder = joinFieldLocalOrder.get(jf.tableIndex).get(jf.fieldIndex).order;
      final TableIterator it = iterators[jf.tableIndex];
      if (localOrder == 0) {
        /* if the join field is highest ordered, reset the cursor */
        it.ranges[jf.fieldIndex].setMinRow(0);
        it.ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
        it.setCurrentField(jf.fieldIndex);
        it.setRowOfCurrentField(0);
      } else {
        /* if the join field is not ordered as the first, set the cursor to last level */
        final int lastJf = localOrderedJoinField.get(jf.tableIndex).get(localOrder - 1).fieldIndex;
        it.ranges[jf.fieldIndex].setMinRow(it.ranges[lastJf].getMinRow());
        it.ranges[jf.fieldIndex].setMaxRow(it.ranges[lastJf].getMaxRow());
        it.setCurrentField(jf.fieldIndex);
        it.setRowOfCurrentField(it.ranges[jf.fieldIndex].getMinRow());
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
  private boolean leapfrogSearch() {
    boolean atEnd = false;
    Preconditions.checkElementIndex(currentDepth, joinFieldMapping.size(), "current depth is invalid.");
    JoinField fieldWithMaxKey =
        joinFieldMapping.get(currentDepth).get(
            (currentIteratorIndex - 1 + joinFieldMapping.get(currentDepth).size())
                % joinFieldMapping.get(currentDepth).size());
    CellPointer maxKey =
        new CellPointer(fieldWithMaxKey.tableIndex, fieldWithMaxKey.fieldIndex, iterators[fieldWithMaxKey.tableIndex]
            .getRowOfCurrentField());

    Preconditions.checkState(
        maxKey.getRow() <= iterators[fieldWithMaxKey.tableIndex].ranges[fieldWithMaxKey.fieldIndex].getMaxRow(),
        "violate max row during search.");
    if (maxKey.getRow() == iterators[fieldWithMaxKey.tableIndex].ranges[fieldWithMaxKey.fieldIndex].getMaxRow()) {
      return true;
    }

    while (true) {
      JoinField fieldWithLeastKey = joinFieldMapping.get(currentDepth).get(currentIteratorIndex);
      CellPointer leastKey =
          new CellPointer(fieldWithLeastKey.tableIndex, fieldWithLeastKey.fieldIndex,
              iterators[fieldWithLeastKey.tableIndex].getRowOfCurrentField());
      if (cellCompare(leastKey, maxKey) == 0) { // if the value current
        break;
      } else {
        atEnd = leapfrogSeek(fieldWithLeastKey, maxKey);
        if (atEnd) {
          break;
        } else {
          // if leapfrog_seek hasn't reach end, update max key, move to the next table
          maxKey =
              new CellPointer(fieldWithLeastKey.tableIndex, fieldWithLeastKey.fieldIndex,
                  iterators[fieldWithLeastKey.tableIndex].getRowOfCurrentField());
          currentIteratorIndex = (currentIteratorIndex + 1) % joinFieldMapping.get(currentDepth).size();
        }
      }
    }
    return atEnd;
  }

  /**
   * @param jf JoinField
   * @throws DbException
   */
  private void refineRange(final JoinField jf) {

    int startRow = iterators[jf.tableIndex].getRow(jf.fieldIndex);
    int endRow = iterators[jf.tableIndex].ranges[jf.fieldIndex].getMaxRow() - 1;
    iterators[jf.tableIndex].ranges[jf.fieldIndex].setMinRow(startRow);
    Preconditions.checkState(startRow <= endRow, "startRow must smaller than endRow");

    final CellPointer startCursor = new CellPointer(jf.tableIndex, jf.fieldIndex, startRow);

    /* short cut: if the maxCursor has the same value as current line */
    CellPointer cursor = new CellPointer(jf.tableIndex, jf.fieldIndex, endRow);
    if (cellCompare(startCursor, cursor) == 0) {
      return;
    }

    /* short cut: if the next line has different value */
    cursor.setRow(++startRow);
    if (cellCompare(startCursor, cursor) < 0) {
      iterators[jf.tableIndex].ranges[jf.fieldIndex].maxRow = startRow;
      return;
    }

    /* refine start */
    int step = 1;
    while (true) {
      int compare = cellCompare(startCursor, cursor);
      Preconditions.checkState(compare <= 0,
          "startCursor must point to an element that is not greater than element pointed by current cursor");
      if (compare < 0) {
        endRow = cursor.getRow();
        break;
      } else if (compare == 0) {
        startRow = cursor.getRow();
        cursor.setRow(startRow + step);
        step = step * 2;
        if (cursor.getRow() + step > endRow) {
          break;
        }
      }
    }

    /* refine end */
    while (true) {
      cursor.setRow((startRow + endRow) / 2);
      int compare = cellCompare(startCursor, cursor);
      if (compare == 0) { // if current cursor equals to start cursor
        startRow = cursor.getRow();
      } else if (compare < 0) { // if current cursor is greater than start cursor
        endRow = cursor.getRow();
      }

      if (endRow == startRow + 1) {
        iterators[jf.tableIndex].ranges[jf.fieldIndex].setMaxRow(endRow);
        return;
      }
    }

  }

  /**
   * move the iterator to the element which is the first key larger than current max.
   * 
   * @param jf seek on which field of which table.
   * @param target the target value of seeking.
   * @return at end or not.
   */
  private boolean leapfrogSeek(final JoinField jf, final CellPointer target) {

    int startRow = iterators[jf.tableIndex].getRow(jf.fieldIndex);
    int endRow = iterators[jf.tableIndex].ranges[jf.fieldIndex].getMaxRow() - 1;
    Preconditions.checkState(startRow <= endRow, "startRow must be no less than endRow");

    final CellPointer startCursor = new CellPointer(jf.tableIndex, jf.fieldIndex, startRow);
    CellPointer cursor = new CellPointer(startCursor);

    /* short-cuts, return result early. */
    if (cellCompare(startCursor, target) >= 0) {
      return false;
    }

    /* set row number to upper bound */
    cursor.setRow(endRow);
    if (cellCompare(cursor, target) < 0) {
      return true;
    }

    /* binary search: find the first row whose value is not less than target */
    while (true) {
      cursor.setRow((endRow + startRow) / 2);
      int compare = cellCompare(cursor, target);
      if (compare >= 0) { // cursor > target
        endRow = cursor.getRow();
      } else if (compare < 0) { // cursor < target
        startRow = cursor.getRow();
      }

      if (startRow == endRow - 1) {
        cursor.setRow(endRow);
        iterators[jf.tableIndex].setRow(jf.fieldIndex, endRow);
        return false;
      }
    }
  }

  /**
   * Leapfrog join.
   */
  private void leapfrogJoin() {

    /* initiate the join for the first time */
    if (currentDepth == -1) {
      currentDepth = 0;
      leapfrogInit();
    }

    /* break if a full tuple batch has been formed */
    while (ansTBB.numTuples() < TupleBatch.BATCH_SIZE) {
      for (JoinField jf : joinFieldMapping.get(currentDepth)) {
        Preconditions.checkState(jf.fieldIndex == iterators[jf.tableIndex].currentField,
            "current field invariant is not correct.");
      }
      boolean atEnd = leapfrogSearch();

      if (atEnd && currentDepth == 0) {
        /* if the first join variable reaches end, then the join finish. */
        joinFinished = true;
        break;

      } else if (atEnd) {
        /* reach to the end in current depth, go back to last depth */
        joinUp();

      } else if (currentDepth == joinFieldMapping.size() - 1) {

        /* refine range */
        for (JoinField jf : joinFieldMapping.get(currentDepth)) {
          refineRange(jf);
        }

        /* exhaust all output with current join key */
        exhaustOutput(0);

        /* move to the next value */
        iterators[joinFieldMapping.get(currentDepth).get(currentIteratorIndex).tableIndex].nextValue();
        currentIteratorIndex =
            (currentIteratorIndex - 1 + joinFieldMapping.get(currentDepth).size())
                % joinFieldMapping.get(currentDepth).size();

        /* restore range */
        for (JoinField jf : joinFieldMapping.get(currentDepth)) {
          final int localOrder = joinFieldLocalOrder.get(jf.tableIndex).get(jf.fieldIndex).order;
          final TableIterator it = iterators[jf.tableIndex];
          if (localOrder != 0) {
            final int lastJf = localOrderedJoinField.get(jf.tableIndex).get(localOrder - 1).fieldIndex;
            it.ranges[jf.fieldIndex].setMinRow(it.ranges[lastJf].getMinRow());
            it.ranges[jf.fieldIndex].setMaxRow(it.ranges[lastJf].getMaxRow());
          } else {
            it.ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
          }
        }

      } else {
        /* go to the next join variable. */
        joinOpen();
      }
    }

  }

  /**
   * advance to the next join variable.
   */
  private void joinOpen() {
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      int ti = jf.tableIndex;
      /* set the range for the highest ordered field in a table */
      if (joinFieldLocalOrder.get(ti).get(iterators[ti].currentField).getOrder() == 0) {
        iterators[jf.tableIndex].ranges[jf.fieldIndex].setMinRow(0);
        iterators[jf.tableIndex].ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
      }
      refineRange(jf);
    }
    currentDepth++;
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.tableIndex].setCurrentField(jf.fieldIndex);
      iterators[jf.tableIndex].setRowOfCurrentField(iterators[jf.tableIndex].ranges[jf.fieldIndex].getMinRow());
    }
    leapfrogInit();
  }

  /**
   * backtrack to previous join variable.
   */
  private void joinUp() {

    currentDepth--;

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.tableIndex].setCurrentField(jf.fieldIndex);
    }

    /* move to the next value */
    currentIteratorIndex = 0;
    iterators[joinFieldMapping.get(currentDepth).get(currentIteratorIndex).tableIndex].nextValue();
    currentIteratorIndex =
        (currentIteratorIndex - 1 + joinFieldMapping.get(currentDepth).size())
            % joinFieldMapping.get(currentDepth).size();

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final TableIterator it = iterators[jf.tableIndex];
      final int localOrder = joinFieldLocalOrder.get(jf.tableIndex).get(jf.fieldIndex).order;
      if (localOrder == 0) {
        it.ranges[jf.fieldIndex].setMinRow(0);
        it.ranges[jf.fieldIndex].setMaxRow(tables[jf.tableIndex].numTuples());
      } else {
        final int lastJf = localOrderedJoinField.get(jf.tableIndex).get(localOrder - 1).fieldIndex;
        it.ranges[jf.fieldIndex].setMinRow(it.ranges[lastJf].getMinRow());
        it.ranges[jf.fieldIndex].setMaxRow(it.ranges[lastJf].getMaxRow());
      }
    }

  }

  /**
   * Recursively output all result tuples sharing the same join key(s).
   * 
   * @param index start position of iterators.
   */
  private void exhaustOutput(final int index) {
    JoinField currentJF = joinFieldMapping.get(currentDepth).get(index);
    int currentRow = iterators[currentJF.tableIndex].ranges[currentJF.fieldIndex].minRow;
    for (; currentRow < iterators[currentJF.tableIndex].ranges[currentJF.fieldIndex].maxRow; currentRow++) {
      iterators[currentJF.tableIndex].setRowOfCurrentField(currentRow);
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
      MutableTupleBuffer hashTable = tables[outputFieldMapping.get(i).tableIndex];
      int row = iterators[outputFieldMapping.get(i).tableIndex].getRowOfCurrentField();
      int rowInTB = hashTable.getTupleIndexInContainingTB(row);
      ReadableColumn sourceColumn = hashTable.getColumns(row)[outputFieldMapping.get(i).fieldIndex];
      ansTBB.put(i, sourceColumn, rowInTB);
    }
  }

  /**
   * @param cp1 CellPointer 1
   * @param cp2 CellPointer 2
   * @return result of comparison
   */
  private int cellCompare(final CellPointer cp1, final CellPointer cp2) {
    return tables[cp1.tableIndex].compare(cp1.getFieldIndex(), cp1.getRow(), tables[cp2.getTableIndex()], cp2
        .getFieldIndex(), cp2.getRow());

  }

}
