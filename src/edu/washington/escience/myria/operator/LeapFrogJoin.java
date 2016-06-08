package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 *
 * This operator implement Leap-Frog join algorithm (http://arxiv.org/abs/1210.0481), which takes multiple relations and
 * arbitrary join structure as input.
 *
 * It takes pre-sorted relations as input. The variable ordering must be implied at joinFieldMapping.
 *
 */
public class LeapFrogJoin extends NAryOperator {
  /**
   * required.
   */
  private static final long serialVersionUID = 1L;

  /**
   * {@code {@link #userJoinFieldMapping}[i]} represents the list of JoinField of i-th join variable.
   */
  private final int[][][] userJoinFieldMapping;

  /**
   * whether to create an index on the first joined field in each relation.
   */
  private transient boolean[] indexOnFirst;

  /**
   * {@code {@link #joinFieldMapping}[i]} is the list of JoinFields of i-th join variable.
   */
  private transient List<List<JoinField>> joinFieldMapping;

  /**
   * {@code {@link #joinFieldLocalOrder}[i][j]} stores join field order of j-th field of i-th child's table.
   */
  private transient List<List<JoinAttrOrder>> joinFieldLocalOrder;

  /**
   * {@code {@link #localOrderedJoinField}[i][j]} stores the join field (locally) ordered j of i-th child's table.
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
   * The index of the lastly ordered attribute among those who are participating join.
   */
  private transient List<Integer> lastJoinAttrIdx;

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
   * index on field with first local order in each table.
   */
  private transient IntArrayList[] firstVarIndices;

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
      Preconditions.checkElementIndex(
          row, tables[tableIndex].numTuples(), "row out of bound when setting row");
      this.row = row;
    }

    /**
     * @param tableIndex the table index of this CellPointer
     * @param fieldIndex the field index of this CellPointer
     * @param row row number
     */
    public CellPointer(final int tableIndex, final int fieldIndex, final int row) {
      Preconditions.checkElementIndex(tableIndex, tables.length, "tableIndex exceeds legal range.");
      Preconditions.checkElementIndex(
          fieldIndex, tables[tableIndex].numColumns(), "fieldIndex exceeds legal range.");
      Preconditions.checkState(
          row >= 0 && row <= tables[tableIndex].numTuples(), "row number exceeds legal range.");
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
    private final int table;

    /**
     * index of this field in its owner table.
     */
    private final int column;

    /**
     * Constructor.
     *
     * @param tableIndex table index.
     * @param fieldIndex join field index in a table.
     */
    public JoinField(final int tableIndex, final int fieldIndex) {
      table = tableIndex;
      column = fieldIndex;
    }

    @Override
    public String toString() {
      return table + ":" + column;
    }
  }

  /**
   * Iterator of table, which implements a Trie like interface.
   *
   */
  private class TableIterator {

    /**
     * table index.
     */
    private final int tableIdx;

    /**
     * current field.
     */
    private int currentField = -1;

    /**
     * row number on {@value firstValueIndices[tableIndex]}.
     */
    private int rowOnIndex = 0;

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
      Preconditions.checkElementIndex(
          field, rowIndices.length, "field index cannot exceed number of columns.");
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
      Preconditions.checkArgument(
          row < ranges[currentField].getMaxRow(),
          "row: %s >= maxRow: %s, currentField: %s, tableIdx: %s, curDepth: %s",
          row,
          ranges[currentField].getMaxRow(),
          currentField,
          tableIdx,
          currentDepth);
      Preconditions.checkArgument(
          row >= ranges[currentField].getMinRow(),
          "row: %s < minRow: %s",
          row,
          ranges[currentField].getMinRow());
    }

    /**
     * Set iterator position on given field.
     *
     * @param field the field index.
     * @param row row number.
     */
    public void setRow(final int field, final int row) {
      Preconditions.checkElementIndex(
          field, rowIndices.length, "field index cannot exceed number of columns.");
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
      Preconditions.checkPositionIndex(
          tableIndex, getChildren().length, "table index cannot exceed number of children.");

      tableIdx = tableIndex;
      /* initiate ranges */
      ranges = new IteratorRange[tables[tableIndex].numColumns()];
      for (int i = 0; i < tables[tableIndex].numColumns(); ++i) {
        ranges[i] = new IteratorRange(0, tables[tableIndex].numTuples());
      }

      /* initiate rowIndices */
      rowIndices = new int[tables[tableIndex].numColumns()];
      Arrays.fill(rowIndices, 0);
    }

    /**
     * The legal range of an iterator on a field. This is to simulate the trie data structure.
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
        Preconditions.checkState(
            minRow < maxRow, "minRow >= maxRow. (minRow=%s, ,maxRow=%s)", minRow, maxRow);
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
        Preconditions.checkArgument(maxRow > 0, "maxRow: %s", maxRow);
        Preconditions.checkState(
            minRow < maxRow, "minRow >= maxRow. (minRow=%s, ,maxRow=%s)", minRow, maxRow);
      }

      /**
       * @param minRow minimal row number of this range, inclusive.
       * @param maxRow maximal row number of this range, exclusive.
       */
      public void setRange(final int minRow, final int maxRow) {
        this.minRow = minRow;
        this.maxRow = maxRow;
        Preconditions.checkArgument(maxRow > 0, "maxRow: %s", maxRow);
        Preconditions.checkState(
            minRow < maxRow, "minRow >= maxRow. (minRow=%s, ,maxRow=%s)", minRow, maxRow);
      }

      /**
       * maxRow is unreachable.
       */
      private int maxRow;

      /**
       * @param aIteratorRange another IteratorRange object copying from
       */
      public void setRange(final IteratorRange aIteratorRange) {
        setRange(aIteratorRange.minRow, aIteratorRange.maxRow);
      }

      /**
       * @param minRow minimal row.
       * @param maxRow maximal row.
       */
      public IteratorRange(final int minRow, final int maxRow) {
        Preconditions.checkArgument(maxRow > 0, "maxRow: %s", maxRow);
        this.minRow = minRow;
        this.maxRow = maxRow;
      }
    }
  }

  /**
   * Helper function, check whether a JoinField is ordered first in its own relation.
   *
   * @param jf JoinField
   * @return true if this JoinField is ordered first, false otherwise.
   */
  private boolean isIndexed(final JoinField jf) {
    return indexOnFirst[jf.table] && getLocalOrder(jf) == 0;
  }

  /**
   * Comparator class for sorting iterators.
   */
  private class JoinIteratorCompare implements Comparator<JoinField> {
    @Override
    public int compare(final JoinField o1, final JoinField o2) {
      return TupleUtils.cellCompare(
          tables[o1.table],
          o1.column,
          iterators[o1.table].getRowOfCurrentField(),
          tables[o2.table],
          o2.column,
          iterators[o2.table].getRowOfCurrentField());
    }
  }

  /**
   * the attribute order of a joined column.
   *
   */
  private final class JoinAttrOrder implements Serializable {
    /**
     * required.
     */
    private static final long serialVersionUID = 1L;
    /**
     * order of this attribute.
     */
    private int order;
    /**
     * column index of this attribute.
     */
    private final int colIdx;

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
     * @return column index in a table..
     */
    private int getColumnIdx() {
      return colIdx;
    }

    /**
     * @param order order of this join field.
     * @param columnIdx column index of this join attribute.
     */
    public JoinAttrOrder(final int order, final int columnIdx) {
      this.order = order;
      colIdx = columnIdx;
    }

    @Override
    public String toString() {
      return colIdx + ":" + order;
    }
  }

  /**
   * @param children list of child operators
   * @param joinFieldMapping mapping of join field to child table field
   * @param outputFieldMapping mapping of output field to child table field
   * @param outputColumnNames output column names
   * @param indexOnFirst whether the first join field of a child is indexed or not
   */
  public LeapFrogJoin(
      final Operator[] children,
      final int[][][] joinFieldMapping,
      final int[][] outputFieldMapping,
      final List<String> outputColumnNames,
      final boolean[] indexOnFirst) {
    super(children);
    userJoinFieldMapping = Objects.requireNonNull(joinFieldMapping, "joinFieldMapping");
    Objects.requireNonNull(outputFieldMapping, "outputFieldMapping");
    if (outputColumnNames != null) {
      Preconditions.checkArgument(
          outputFieldMapping.length == outputColumnNames.size(),
          "outputColumns and outputFieldMapping should have the same cardinality.");
    }

    /* set output schema */
    if (outputColumnNames != null) {
      Preconditions.checkArgument(
          ImmutableSet.copyOf(outputColumnNames).size() == outputColumnNames.size(),
          "duplicate names in output schema. ");
      this.outputColumnNames = ImmutableList.copyOf(outputColumnNames);
    } else {
      this.outputColumnNames = null;
    }

    /* set output field */
    this.outputFieldMapping = new ArrayList<JoinField>();
    for (int[] element : outputFieldMapping) {
      Preconditions.checkArgument(
          element.length == 2,
          "An array representing join field must be at length of 2. ([tableIndex,fieldIndex])");
      this.outputFieldMapping.add(new JoinField(element[0], element[1]));
    }

    /* init indexOnFirst */
    this.indexOnFirst = indexOnFirst;
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
    /* Initialization before LeapFrog starts. */
    if (currentDepth == -1) {
      /* handle the case that one of input tables is empty. */
      for (MutableTupleBuffer table : tables) {
        if (table.numTuples() == 0) {
          joinFinished = true;
          checkEOSAndEOI();
          return null;
        }
      }
      /* Initiate table iterators. */
      initIterators();
    }
    /* do the join, pop if there is ready tb. */
    if (!joinFinished) {
      leapfrogJoin();
    }
    TupleBatch nexttb = ansTBB.popAny();

    Preconditions.checkState(joinFinished || nexttb != null, "incorrect return");
    return nexttb;
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
      types.add(
          children[outputFieldMapping.get(i).table]
              .getSchema()
              .getColumnType(outputFieldMapping.get(i).column));
      names.add(
          children[outputFieldMapping.get(i).table]
              .getSchema()
              .getColumnName(outputFieldMapping.get(i).column));
    }
    if (outputColumnNames != null) {
      return new Schema(types.build(), outputColumnNames);
    } else {
      return new Schema(types, names);
    }
  }

  /**
   * move to the next iterator.
   */
  private void nextIterator() {
    currentIteratorIndex = (currentIteratorIndex + 1) % joinFieldMapping.get(currentDepth).size();
  }

  /**
   * Initiate iterators.
   */
  private void initIterators() {
    iterators = new TableIterator[tables.length];
    for (int i = 0; i < tables.length; ++i) {
      iterators[i] = new TableIterator(i);
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    Operator[] children = getChildren();

    /* check indexOnFirst. */
    if (indexOnFirst == null) {
      indexOnFirst = new boolean[children.length];
      for (int i = 0; i < children.length; ++i) {
        indexOnFirst[i] = false;
      }
    }
    Preconditions.checkArgument(
        children.length == indexOnFirst.length,
        "indexOnFirst must have the same cardinality as children.");

    /* initiate join field mapping and field local order */
    joinFieldMapping = new ArrayList<List<JoinField>>();
    joinFieldLocalOrder = new ArrayList<>(children.length);

    for (Operator element : children) {
      List<JoinAttrOrder> localOrder = new ArrayList<>();
      List<JoinAttrOrder> globalOrder = new ArrayList<>();
      for (int i = 0; i < element.getSchema().numColumns(); ++i) {
        localOrder.add(new JoinAttrOrder(-1, i));
        globalOrder.add(new JoinAttrOrder(-1, i));
      }
      joinFieldLocalOrder.add(localOrder);
    }

    /* set join field mapping and field local order */
    for (int i = 0; i < userJoinFieldMapping.length; ++i) {
      List<JoinField> joinedFieldList = new ArrayList<JoinField>();
      for (int j = 0; j < userJoinFieldMapping[i].length; ++j) {
        // get table index and field index of each join field
        Preconditions.checkArgument(
            userJoinFieldMapping[i][j].length == 2,
            "the inner arrary of JoinFieldMapping must have the length of 2");
        final int tableIdx = userJoinFieldMapping[i][j][0];
        final int colIdx = userJoinFieldMapping[i][j][1];
        // update joinFieldMapping and reverseJoinFieldMapping
        Preconditions.checkPositionIndex(
            tableIdx, children.length, "table index cannot exceed the number of children.");
        Preconditions.checkPositionIndex(
            colIdx,
            children[tableIdx].getSchema().numColumns(),
            "field index cannot exceed the number of columns.");
        joinedFieldList.add(new JoinField(tableIdx, colIdx));
        joinFieldLocalOrder.get(tableIdx).get(colIdx).setOrder(i);
      }
      joinFieldMapping.add(joinedFieldList);
    }

    localOrderedJoinField = new ArrayList<>();
    for (int i = 0; i < joinFieldLocalOrder.size(); ++i) {

      List<JoinField> jfl = new ArrayList<>();
      List<JoinAttrOrder> colAttrOrder = new ArrayList<>();
      for (JoinAttrOrder localOrder : joinFieldLocalOrder.get(i)) {
        if (localOrder.getOrder() != -1) {
          colAttrOrder.add(new JoinAttrOrder(localOrder.getOrder(), localOrder.getColumnIdx()));
        }
      }

      Collections.sort(
          colAttrOrder,
          new Comparator<JoinAttrOrder>() {
            @Override
            public int compare(final JoinAttrOrder o1, final JoinAttrOrder o2) {
              return Integer.compare(o1.getOrder(), o2.getOrder());
            }
          });

      for (int j = 0; j < colAttrOrder.size(); j++) {
        jfl.add(new JoinField(i, colAttrOrder.get(j).getColumnIdx()));
      }
      localOrderedJoinField.add(jfl);
    }

    lastJoinAttrIdx = new ArrayList<>(children.length);

    /* convert the global order to the local order and update the local order back to the joinFieldLocalOrder. */
    for (int i = 0; i < localOrderedJoinField.size(); ++i) {
      List<JoinField> orderedJoinField = localOrderedJoinField.get(i);
      for (int j = 0; j < orderedJoinField.size(); j++) {
        JoinField jf = orderedJoinField.get(j);
        joinFieldLocalOrder.get(i).get(jf.column).setOrder(j);
        if (j == orderedJoinField.size() - 1) {
          lastJoinAttrIdx.add(jf.column);
        }
      }
    }

    /* Initiate hash tables and indices */
    tables = new MutableTupleBuffer[children.length];
    firstVarIndices = new IntArrayList[children.length];
    for (int i = 0; i < children.length; ++i) {
      tables[i] = new MutableTupleBuffer(children[i].getSchema());
      firstVarIndices[i] = new IntArrayList();
    }

    currentDepth = -1;

    ansTBB = new TupleBatchBuffer(getSchema());
  }

  @Override
  protected void cleanup() throws DbException {
    Operator[] children = getChildren();
    for (int i = 0; i < children.length; ++i) {
      tables[i] = null;
      firstVarIndices[i] = null;
    }
    tables = null;
    firstVarIndices = null;
    /* iterators may not be initialized */
    if (iterators != null) {
      for (int i = 0; i < iterators.length; ++i) {
        iterators[i] = null;
      }
      iterators = null;
    }
    ansTBB = null;
    lastJoinAttrIdx = null;
  }

  /**
   * @param childIndex which child to store
   * @param tb incoming tuple
   */
  private void storeChildTuple(final int childIndex, final TupleBatch tb) {
    List<? extends Column<?>> inputColumns = tb.getDataColumns();
    for (int row = 0; row < tb.numTuples(); ++row) {
      for (int column = 0; column < tb.numColumns(); column++) {
        tables[childIndex].put(column, inputColumns.get(column), row);
        /* put value to index table if it first appears */
        if (isIndexed(new JoinField(childIndex, column))) {
          int thisRow = tables[childIndex].numTuples();
          /*
           * If this is the last column, this tuple is already built in tables[childIndex]. So numTuples() have been
           * updated already.
           */
          if (column == tb.numColumns() - 1) {
            thisRow--;
          }
          int lastRow = thisRow - 1;
          if (lastRow == -1
              || TupleUtils.cellCompare(tables[childIndex], column, lastRow, tb, column, row)
                  != 0) {
            firstVarIndices[childIndex].add(thisRow);
          }
        }
      }
    }
  }

  /**
   * init/restart leap-frog join.
   */
  private void leapfrogInit() {

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final int localOrder = getLocalOrder(jf);
      final TableIterator it = iterators[jf.table];
      if (localOrder == 0) {
        /* if the join field is highest ordered, reset the cursor */
        it.ranges[jf.column].setRange(0, tables[jf.table].numTuples());
        it.setCurrentField(jf.column);
        it.setRowOfCurrentField(0);
        it.rowOnIndex = 0;
      } else {
        /* if the join field is not ordered as the first, set the cursor to last level */
        final int lastJf = localOrderedJoinField.get(jf.table).get(localOrder - 1).column;
        it.ranges[jf.column].setRange(it.ranges[lastJf]);
        it.setCurrentField(jf.column);
        it.setRowOfCurrentField(it.ranges[jf.column].getMinRow());
      }
    }

    Collections.sort(joinFieldMapping.get(currentDepth), new JoinIteratorCompare());
    currentIteratorIndex = joinFieldMapping.get(currentDepth).size() - 1;
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
    Preconditions.checkElementIndex(
        currentDepth, joinFieldMapping.size(), "current depth is invalid.");
    /* get the column to proceed the search. */
    JoinField columnToProceed = joinFieldMapping.get(currentDepth).get(currentIteratorIndex);
    CellPointer maxKey =
        new CellPointer(
            columnToProceed.table,
            columnToProceed.column,
            iterators[columnToProceed.table].getRowOfCurrentField());
    /* if this is already the end of a trie range, return atEnd=ture. */
    final int maxRow = iterators[columnToProceed.table].ranges[columnToProceed.column].getMaxRow();
    Preconditions.checkState(
        maxKey.getRow() <= maxRow, "current row: %s, maxRow: %s", maxKey.getRow(), maxRow);
    if (maxKey.getRow() == maxRow) {
      return true;
    }

    nextIterator();

    while (true) {
      JoinField fieldWithLeastKey = joinFieldMapping.get(currentDepth).get(currentIteratorIndex);
      CellPointer leastKey =
          new CellPointer(
              fieldWithLeastKey.table,
              fieldWithLeastKey.column,
              iterators[fieldWithLeastKey.table].getRowOfCurrentField());
      if (cellCompare(leastKey, maxKey) == 0) { // if the value current
        break;
      } else {
        atEnd = leapfrogSeek(fieldWithLeastKey, maxKey);
        if (atEnd) {
          break;
        } else {
          // if leapfrog_seek hasn't reach end, update max key, move to the next table
          maxKey =
              new CellPointer(
                  fieldWithLeastKey.table,
                  fieldWithLeastKey.column,
                  iterators[fieldWithLeastKey.table].getRowOfCurrentField());
          nextIterator();
        }
      }
    }
    /* checking the state */
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final int maxRowJf = iterators[jf.table].ranges[jf.column].getMaxRow();
      final int minRowJf = iterators[jf.table].ranges[jf.column].getMinRow();
      final int curRow = iterators[jf.table].getRow(jf.column);
      Preconditions.checkState(curRow >= minRowJf, "curRow: %s, minRow: %s", curRow, minRowJf);
      Preconditions.checkState(curRow < maxRowJf, "curRow: %s, maxRow: %s", curRow, maxRowJf);
    }

    return atEnd;
  }

  /**
   * @param jf JoinField
   * @throws DbException
   */
  private void refineRange(final JoinField jf) {

    int startRow = iterators[jf.table].getRow(jf.column);
    int endRow = iterators[jf.table].ranges[jf.column].getMaxRow() - 1;
    iterators[jf.table].ranges[jf.column].setMinRow(startRow);
    Preconditions.checkState(
        startRow <= endRow, "startRow > endRow. (startRow=%s, endRow=%s)", startRow, endRow);

    final CellPointer startCursor = new CellPointer(jf.table, jf.column, startRow);

    /* short cut: if the maxCursor has the same value as current line */
    CellPointer cursor = new CellPointer(jf.table, jf.column, endRow);
    if (cellCompare(startCursor, cursor) == 0) {
      return;
    }

    /* short cut: if the next line has different value */
    cursor.setRow(++startRow);
    if (cellCompare(startCursor, cursor) < 0) {
      iterators[jf.table].ranges[jf.column].setMaxRow(startRow);
      return;
    }

    /* refine start */
    int step = 1;
    while (true) {
      int compare = cellCompare(startCursor, cursor);
      Preconditions.checkState(
          compare <= 0,
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
        iterators[jf.table].ranges[jf.column].setMaxRow(endRow);
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
    /* switch to indexed version if possible. */
    if (isIndexed(jf)) {
      return leapfrogSeekWithIndex(jf, target);
    }

    int startRow = iterators[jf.table].getRow(jf.column);
    int endRow = iterators[jf.table].ranges[jf.column].getMaxRow() - 1;
    Preconditions.checkState(startRow <= endRow, "startRow must be no less than endRow");

    final CellPointer startCursor = new CellPointer(jf.table, jf.column, startRow);
    CellPointer cursor = new CellPointer(startCursor);

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
        iterators[jf.table].setRow(jf.column, endRow);
        return false;
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
  private boolean leapfrogSeekWithIndex(final JoinField jf, final CellPointer target) {
    IntArrayList index = firstVarIndices[jf.table];
    int startRowOnIndex = iterators[jf.table].rowOnIndex;
    int endRowOnIndex = index.size() - 1;
    Preconditions.checkState(
        startRowOnIndex <= endRowOnIndex, "startRow must be no less than endRow");
    Preconditions.checkElementIndex(startRowOnIndex, index.size());
    Preconditions.checkElementIndex(endRowOnIndex, index.size());

    final CellPointer startCursor =
        new CellPointer(jf.table, jf.column, index.get(startRowOnIndex));
    CellPointer cursor = new CellPointer(startCursor);

    /* set row number to upper bound */
    cursor.setRow(index.get(endRowOnIndex));
    if (cellCompare(cursor, target) < 0) {
      return true;
    }

    /* binary search: find the first row whose value is not less than target */
    while (true) {
      int rowOnIndex = (startRowOnIndex + endRowOnIndex) / 2;
      cursor.setRow(index.get(rowOnIndex));
      int compare = cellCompare(cursor, target);
      if (compare >= 0) { // cursor > target
        endRowOnIndex = rowOnIndex;
      } else if (compare < 0) { // cursor < target
        startRowOnIndex = rowOnIndex;
      }

      if (startRowOnIndex == endRowOnIndex - 1) {
        cursor.setRow(index.get(endRowOnIndex));
        iterators[jf.table].setRow(jf.column, index.get(endRowOnIndex));
        iterators[jf.table].rowOnIndex = endRowOnIndex;
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
        Preconditions.checkState(
            jf.column == iterators[jf.table].currentField,
            "current field invariant is not correct.");
      }
      /* do LeapFrog search to find the next output position. */
      boolean atEnd = leapfrogSearch();
      if (atEnd && currentDepth == 0) {
        /* if the first join variable reaches end, then the join finish. */
        joinFinished = true;
        break;
      } else if (atEnd) {
        /* reach to the end in current depth, go back to last depth */
        joinUp();
      } else if (currentDepth == joinFieldMapping.size() - 1) {
        /* output all the tuples on this position and move to the next value. */
        /* 1. refine ranges. */
        for (JoinField jf : joinFieldMapping.get(currentDepth)) {
          refineRange(jf);
        }

        /* 2. exhaust all output with current join key. */
        exhaustOutput(0);

        /* 3. move to the next value */
        iterators[joinFieldMapping.get(currentDepth).get(currentIteratorIndex).table].nextValue();

        /* 4. restore ranges */
        for (JoinField jf : joinFieldMapping.get(currentDepth)) {
          final int localOrder = getLocalOrder(jf);
          final TableIterator it = iterators[jf.table];
          if (localOrder != 0) {
            final int lastJf = localOrderedJoinField.get(jf.table).get(localOrder - 1).column;
            it.ranges[jf.column].setRange(it.ranges[lastJf]);
          } else {
            it.ranges[jf.column].setMaxRow(tables[jf.table].numTuples());
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
      /* refine the range of join variable at current depth */
      refineRange(jf);
    }
    currentDepth++;
    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.table].setCurrentField(jf.column);
      iterators[jf.table].setRowOfCurrentField(iterators[jf.table].ranges[jf.column].getMinRow());
    }
    leapfrogInit();
  }

  /**
   * @param jf JoinField
   * @return the local order of the JoinField jf, starting from 0.
   */
  private int getLocalOrder(final JoinField jf) {
    return joinFieldLocalOrder.get(jf.table).get(jf.column).getOrder();
  }

  /**
   * backtrack to previous join variable.
   */
  private void joinUp() {

    currentDepth--;

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      iterators[jf.table].setCurrentField(jf.column);
    }

    /* arbitrarily choose one relation, let's say, 0th. */
    currentIteratorIndex = 0;
    /* move its cursor to the next value. */
    iterators[joinFieldMapping.get(currentDepth).get(currentIteratorIndex).table].nextValue();

    for (JoinField jf : joinFieldMapping.get(currentDepth)) {
      final TableIterator it = iterators[jf.table];
      final int localOrder = getLocalOrder(jf);
      if (localOrder == 0) {
        it.ranges[jf.column].setRange(0, tables[jf.table].numTuples());
      } else {
        final int lastJf = localOrderedJoinField.get(jf.table).get(localOrder - 1).column;
        it.ranges[jf.column].setRange(it.ranges[lastJf]);
      }
    }
  }

  /**
   * Recursively output all result tuples sharing the same join key(s).
   *
   * @param index the current table index.
   */
  private void exhaustOutput(final int index) {
    final int tableIdx = index;
    final int colIdx = lastJoinAttrIdx.get(index);
    int currentRow = iterators[tableIdx].ranges[colIdx].minRow;
    for (; currentRow < iterators[tableIdx].ranges[colIdx].maxRow; currentRow++) {
      iterators[tableIdx].setRowOfCurrentField(currentRow);
      if (index == tables.length - 1) {
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
      MutableTupleBuffer hashTable = tables[outputFieldMapping.get(i).table];
      int row = iterators[outputFieldMapping.get(i).table].getRowOfCurrentField();
      int rowInTB = hashTable.getTupleIndexInContainingTB(row);
      ReadableColumn sourceColumn = hashTable.getColumns(row)[outputFieldMapping.get(i).column];
      ansTBB.put(i, sourceColumn, rowInTB);
    }
  }

  /**
   * @param cp1 CellPointer 1
   * @param cp2 CellPointer 2
   * @return result of comparison
   */
  private int cellCompare(final CellPointer cp1, final CellPointer cp2) {
    return TupleUtils.cellCompare(
        tables[cp1.tableIndex],
        cp1.getFieldIndex(),
        cp1.getRow(),
        tables[cp2.getTableIndex()],
        cp2.getFieldIndex(),
        cp2.getRow());
  }

  /**
   * @return number of tuples in all the hash tables.
   */
  public long getNumTuplesInHashTables() {
    if (tables == null) {
      return 0L;
    }
    long sum = 0L;
    for (MutableTupleBuffer table : tables) {
      if (table != null) {
        sum += table.numTuples();
      }
    }
    return sum;
  }
}
