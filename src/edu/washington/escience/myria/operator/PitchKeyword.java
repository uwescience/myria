package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * PitchKeyword is an operator that takes a tuple representing
 * (song_id, beat_number, pitch 0 - 11)
 * and produces an integer keyword representing the pitches
 * that are present, as described in
 * http://www.nature.com/srep/2012/120726/srep00521/full/srep00521.html
 */
public final class PitchKeyword extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Create logger for info logging below.
   */
  // private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
  // .getLogger(ApplyEStep.class);

  /**
   * Which columns to sort the tuples by.
   */

  // TODO(hyrkas): change or delete
  private final String[] columnNames = {"sond_id", "beat_number", "keyword"};
  private final int numDimensions = 12;

  /**
   * Constructor accepts a predicate to apply and a child operator to read
   * tuples to filter from.
   * 
   * @param child
   *            The child operator
   */
  public PitchKeyword(final Operator child) {
    super(child);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    Operator child = getChild();

    TupleBatch tb = child.nextReady();
    if (tb == null) {
      return null;
    }

    // Create new column builder to which to append output
    LongColumnBuilder builder = new LongColumnBuilder();

    for (int row = 0; row < tb.numTuples(); ++row) {
      List<? extends Column<?>> inputColumns = tb.getDataColumns();

      // Read the data into data structures for computation
      int indexCounter = 0;

      // song_id is the first column
      String song_id = inputColumns.get(indexCounter).getString(row);
      indexCounter++;

      // beat_id is the second column
      long beat_id = inputColumns.get(indexCounter).getLong(row);
      indexCounter++;

      double[] pitches = new double[numDimensions];
      for (int i = 0; i < numDimensions; i++) {
        pitches[i] = inputColumns.get(indexCounter).getDouble(row);
        indexCounter++;
      }

      long output = getPitchKeyword(pitches);
      builder.appendLong(output);
    }

    // TODO(hyrkas): create a new schema here
    TupleBatch firstTwoColumns = tb.selectColumns(
      new int[] {0, 1}, new Schema(ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE),
                         ImmutableList.of("song_id", "beat")));

    return firstTwoColumns.appendColumn("keyword", builder.build());
  }

  private long getPitchKeyword(double[] pitches) {
    long keyword = 0;
    for (int i = 0; i < pitches.length; i++) {
      int tmp = pitches[i] >= 0.5 ? 1 : 0;
      tmp = tmp << (11 - i);
      keyword += tmp;
    }
    return keyword;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars)
      throws DbException {
    // LOGGER.info("From FilterEStep - Reached init.");
  }

  @Override
  public Schema generateSchema() {
    return new Schema(
      ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
      ImmutableList.of("song_id", "beat", "keyword")
    );
  }

}
