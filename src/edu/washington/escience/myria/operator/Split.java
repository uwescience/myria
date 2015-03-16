package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * Operator which splits a string-valued column on a Java regular expression and duplicates the input row with each
 * segment of the split result.
 * 
 * E.g., (1, 2, "foo:bar:baz") -> (1, 2, "foo"), (1, 2, "bar"), (1, 2, "baz")
 */
public class Split extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Index of (string-valued) column to split on regex.
   */
  private final int splitColumnIndex;

  /**
   * Compiled regex to split input tuples.
   */
  private final Pattern pattern;

  /**
   * Schema of output tuples.
   */
  private Schema outputSchema;

  /**
   * Buffer to hold finished and in-progress TupleBatches.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param columnName name of string column to split using regex
   * @param regex regular expression to split string input column on
   */
  public Split(final Operator child, final int splitColumnIndex, final String regex) {
    super(child);
    this.splitColumnIndex = splitColumnIndex;
    pattern = Pattern.compile(regex);
  }

  private TupleBatch getChildBatch() throws DbException {
    TupleBatch childTuples;
    try {
      childTuples = getChild().fetchNextReady();
    } catch (Exception e) {
      // only wrap checked exceptions that do not extend DbException
      Throwables.propagateIfPossible(e, DbException.class);
      throw new DbException(e);
    }
    return childTuples;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // If there's a batch already finished, return it, otherwise keep reading
    // batches from the child until we have a full batch or the child returns null.
    TupleBatch inputTuples;
    while (!outputBuffer.hasFilledTB() && (inputTuples = getChildBatch()) != null) {
      for (int rowIdx = 0; rowIdx < inputTuples.numTuples(); ++rowIdx) {
        String colValue = inputTuples.getString(splitColumnIndex, rowIdx);
        // We must specify a negative value for the limit parameter to avoid discarding trailing empty strings:
        // http://docs.oracle.com/javase/7/docs/api/java/lang/String.html#split(java.lang.String,%20int)
        String[] splits = pattern.split(colValue, -1);
        for (String segment : splits) {
          outputBuffer.putString(splitColumnIndex, segment);
          // For each split segment, duplicate the values of all other columns in this row.
          for (int colIdx = 0; colIdx < inputTuples.numColumns(); ++colIdx) {
            if (colIdx != splitColumnIndex) {
              TupleUtils.copyValue(inputTuples.asColumn(colIdx), rowIdx, outputBuffer, colIdx);
            }
          }
        }
      }
    }
    // If we produced a full batch, return it, otherwise finish the current batch and return it.
    return outputBuffer.popAny();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator child = getChild();
    Preconditions.checkNotNull(child, "Child operator cannot be null");
    final Schema inputSchema = child.getSchema();
    Preconditions.checkNotNull(inputSchema, "Child schema cannot be null");
    final Type colType = inputSchema.getColumnType(splitColumnIndex);
    Preconditions.checkState(colType == Type.STRING_TYPE,
        "Column to split at index %d (%s) must have type STRING_TYPE", splitColumnIndex, colType);

    outputBuffer = new TupleBatchBuffer(generateSchema());
  }

  @Override
  public Schema generateSchema() {
    final Schema inputSchema = getChild().getSchema();
    final List<String> outputColumnNames = new ArrayList<>(inputSchema.getColumnNames());
    final String splitColumnName = outputColumnNames.get(splitColumnIndex);
    final String splitResultsColumnName = splitColumnName + "_splits";
    outputColumnNames.set(splitColumnIndex, splitResultsColumnName);
    return Schema.of(inputSchema.getColumnTypes(), outputColumnNames);
  }
}
