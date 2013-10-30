package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;

public class MultiwayJoin extends NAryOperator {

  /**
   * required.
   */
  private static final long serialVersionUID = 1L;

  /**
   * data source to be joined.
   */
  private final List<Operator> children;

  /**
   * stores mapping of output field to fields of child tables. E.g. joinFieldsMapping[0]: List of joined fields in child
   * tables and will be the first column in output, etc.
   * 
   */
  private final List<List<JoinField>> joinFieldMapping;

  /**
   * stores mapping of a field of child table to output field. E.g. reverseJoinFieldMapping[0][0]: The output field
   * mapped to the first field of the first child.
   */
  private final List<int[]> reverseJoinFieldMapping;

  /**
   * output column names.
   */
  private final ImmutableList<String> outputColumnNames;

  /**
   * 
   * Indicate a filed in a child table
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
   * comparator class for JoinField
   * 
   */
  private class JoinFieldCompare implements Comparator<JoinField> {
    @Override
    public int compare(JoinField o1, JoinField o2) {
      if (o1.tableIndex <= o1.tableIndex) {
        return 0;
      } else {
        return 1;
      }
    }

  }

  /**
   * @param children list of child operators
   * @param joinFieldMapping mapping of output field to child table field
   * @param outputColumns output column names
   */
  public MultiwayJoin(final List<Operator> children, final List<List<List<Integer>>> joinFieldMapping,
      final List<String> outputColumns) {
    if (outputColumns != null) {
      Preconditions.checkArgument(joinFieldMapping.size() == outputColumns.size(),
          "outputColumns and JoinFieldMapping should have the same cardinality.");
    }
    /* set children */
    this.children = children;

    /* set join field mapping and reverse field mapping */
    this.joinFieldMapping = new ArrayList<List<JoinField>>();
    reverseJoinFieldMapping = new ArrayList<int[]>(children.size());
    for (int i = 0; i < children.size(); ++i) {
      reverseJoinFieldMapping.add(new int[children.get(i).getSchema().numColumns()]);
    }
    for (int i = 0; i < joinFieldMapping.size(); ++i) {
      List<JoinField> joinedFieldList = new ArrayList<JoinField>();
      for (int j = 0; j < joinFieldMapping.get(i).size(); ++j) {
        // get table index and field index of each join field
        Preconditions.checkArgument(joinFieldMapping.get(i).get(j).size() == 2);
        int tableIndex = joinFieldMapping.get(i).get(j).get(0);
        int fieldIndex = joinFieldMapping.get(i).get(j).get(1);
        // update joinFieldMapping and reverseJoinFieldMapping
        Preconditions.checkPositionIndex(tableIndex, children.size());
        Preconditions.checkPositionIndex(fieldIndex, children.get(tableIndex).getSchema().numColumns());
        joinedFieldList.add(new JoinField(tableIndex, fieldIndex));
        reverseJoinFieldMapping.get(tableIndex)[fieldIndex] = i;
      }
      Collections.sort(joinedFieldList, new JoinFieldCompare());
      this.joinFieldMapping.add(joinedFieldList);
    }

    /* set output schema */
    if (outputColumns != null) {
      Preconditions.checkArgument(ImmutableSet.copyOf(outputColumns).size() == outputColumns.size(),
          "duplicate names in output schema. ");
      outputColumnNames = ImmutableList.copyOf(outputColumns);
    } else {
      outputColumnNames = null;
    }

    if (children.size() > 0) {
      generateSchema();
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    return null;
  }

  @Override
  protected Schema generateSchema() {
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i = 0; i < joinFieldMapping.size(); ++i) {
      types.add(children.get(joinFieldMapping.get(i).get(0).tableIndex).getSchema().getColumnType(
          joinFieldMapping.get(i).get(0).fieldIndex));
      names.add(children.get(joinFieldMapping.get(i).get(0).tableIndex).getSchema().getColumnName(
          joinFieldMapping.get(i).get(0).fieldIndex));
    }
    if (outputColumnNames != null) {
      return new Schema(types.build(), outputColumnNames);
    } else {
      return new Schema(types, names);
    }
  }

}
