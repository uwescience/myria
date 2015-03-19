package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * Operator which splits a string-valued column on a Java regular expression and duplicates the input row with each
 * segment of the split result.
 * 
 * E.g., (1, 2, "foo:bar:baz") -> (1, 2, "foo"), (1, 2, "bar"), (1, 2, "baz")
 */
public final class Split extends UnaryOperator {
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
   * Buffer to hold finished and in-progress TupleBatches.
   */
  private TupleBatchBuffer outputBuffer;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param splitColumnIndex index of string column to split using {@link #regex}
   * @param regex regular expression to split value of column at {@link #splitColumnIndex}
   */
  public Split(@Nonnull final Operator child, final int splitColumnIndex, @Nonnull final String regex) {
    super(child);
    Preconditions.checkNotNull(child, "Child operator cannot be null");
    this.splitColumnIndex = splitColumnIndex;
    Preconditions.checkNotNull(regex, "Regular expression string cannot be null");
    pattern = Pattern.compile(regex);
  }

  @Override
  @Nullable
  protected TupleBatch fetchNextReady() throws DbException {
    // If there's a batch already finished, return it, otherwise keep reading
    // batches from the child until we have a full batch or the child returns null.
    while (!outputBuffer.hasFilledTB()) {
      TupleBatch inputTuples = getChild().nextReady();
      if (inputTuples == null) {
        break;
      }
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
    final Schema inputSchema = child.getSchema();
    Preconditions.checkNotNull(inputSchema, "Child schema cannot be null");
    final Type colType = inputSchema.getColumnType(splitColumnIndex);
    Preconditions.checkState(colType == Type.STRING_TYPE,
        "Column to split at index %d (%s) must have type STRING_TYPE", splitColumnIndex, colType);

    outputBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  @Nonnull
  public Schema generateSchema() {
    final Schema inputSchema = getChild().getSchema();
    final List<String> outputColumnNames = new ArrayList<>(inputSchema.getColumnNames());
    final String splitColumnName = outputColumnNames.get(splitColumnIndex);
    final String splitResultsColumnName = splitColumnName + "_splits";
    outputColumnNames.set(splitColumnIndex, splitResultsColumnName);
    return Schema.of(inputSchema.getColumnTypes(), outputColumnNames);
  }
}
