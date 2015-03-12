package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * Generic apply operator.
 */
public class Split extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Name of column to split on regex. Must have String type.
   */
  private final String columnName;

  /**
   * Compiled regex to split input tuples.
   */
  private final Pattern pattern;

  /**
   * Schema of output tuples.
   */
  private Schema outputSchema;

  /**
   * Index of column to split in input schema (and anonymous result column in output schema).
   */
  private int splitColIdx;

  /**
   * Array of column indices in output schema corresponding to column indices of input schema. The column to be split
   * has its index mapped to the sentinel value -1.
   */
  private int[] inputColIndexToOutputColIndex;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param regex regular expression to split string input column on
   */
  public Split(final Operator child, final String columnName, final String regex) {
    super(child);
    this.columnName = columnName;
    pattern = Pattern.compile(regex);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    for (TupleBatch tb = child.nextReady(); tb != null; tb = child.nextReady()) {
      List<ColumnBuilder<?>> colBuilders = new ArrayList<>(tb.numColumns());
      for (int i = 0; i < tb.numColumns(); ++i) {
        if (i != splitColIdx) {
          colBuilders.add(ColumnFactory.allocateColumn(tb.asColumn(i).getType()));
        }
      }
      final ColumnBuilder<?> splitColBuilder = ColumnFactory.allocateColumn(Type.STRING_TYPE);
      colBuilders.add(splitColBuilder);
      for (int rowIdx = 0; rowIdx < tb.numTuples(); ++rowIdx) {
        try {
          String colValue = tb.asColumn(splitColIdx).getString(rowIdx);
          String[] splits = pattern.split(colValue);
          for (String segment : splits) {
            splitColBuilder.appendString(segment);
            // For each split segment, duplicate the values of all other columns in this row.
            for (int inputColIdx = 0; inputColIdx < inputColIndexToOutputColIndex.length; ++inputColIdx) {
              int outputColIdx = inputColIndexToOutputColIndex[inputColIdx];
              if (outputColIdx != -1) {
                TupleUtils.copyValue(tb.asColumn(inputColIdx), rowIdx, colBuilders.get(outputColIdx));
              }
            }
          }
        } catch (Exception e) {
          throw new DbException(e);
        }
      }
      // TODO: this should be factored out into a common utility method
      List<Column<?>> columns = new ArrayList<>();
      for (ColumnBuilder<?> colBuilder : colBuilders) {
        columns.add(colBuilder.build());
      }
      return new TupleBatch(outputSchema, columns);
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator child = getChild();
    Preconditions.checkNotNull(child, "Child operator cannot be null");
    final Schema inputSchema = child.getSchema();
    Preconditions.checkNotNull(inputSchema, "Child schema cannot be null");
    splitColIdx = inputSchema.columnNameToIndex(columnName);
    final Type colType = inputSchema.getColumnType(splitColIdx);
    Preconditions.checkState(colType == Type.STRING_TYPE, "Column to split \"" + columnName
        + "\" must have type STRING_TYPE but has type " + colType);

    // The only difference between input and output schema is that the input column being split is removed
    // from the output schema and the (anonymous) output column containing split results appears at the
    // final ordinal position.
    inputColIndexToOutputColIndex = new int[inputSchema.numColumns()];
    int[] colsToKeep = new int[inputSchema.numColumns() - 1];
    for (int colIdx = 0; colIdx < inputSchema.numColumns(); ++colIdx) {
      if (colIdx == splitColIdx) {
        // Omit split column index from list of columns of output schema.
        // -1 is a sentinel value indicating that the split column results appear in the last column of the output
        // schema.
        inputColIndexToOutputColIndex[colIdx] = -1;
      } else if (colIdx > splitColIdx) {
        // Offset output column index to compensate for deleted split column index.
        inputColIndexToOutputColIndex[colIdx] = colIdx - 1;
        colsToKeep[colIdx - 1] = colIdx;
      } else {
        inputColIndexToOutputColIndex[colIdx] = colIdx;
        colsToKeep[colIdx] = colIdx;
      }
    }
    final Schema subSchema = inputSchema.getSubSchema(colsToKeep);
    outputSchema = Schema.appendColumn(subSchema, Type.STRING_TYPE, "_splits");
  }

  @Override
  public Schema generateSchema() {
    return outputSchema;
  }
}
