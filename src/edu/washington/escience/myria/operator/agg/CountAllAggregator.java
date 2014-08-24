package edu.washington.escience.myria.operator.agg;

import com.google.common.math.LongMath;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * An aggregator that counts the number of rows in its input.
 */
public final class CountAllAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The number of tuples seen so far. */
  private long count;
  /** The schema of the aggregate results. */
  public static final Schema SCHEMA = Schema.ofFields(Type.LONG_TYPE, "count_all");

  /** Initialize this Aggregator. */
  public CountAllAggregator() {
    count = 0;
  }

  @Override
  public void add(final ReadableTable from) throws DbException {
    count = LongMath.checkedAdd(count, from.numTuples());
  }

  @Override
  public void addRow(final ReadableTable from, final int row) throws DbException {
    count = LongMath.checkedAdd(count, 1);
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn) throws DbException {
    dest.putLong(destColumn, count);
  }

  @Override
  public Schema getResultSchema() {
    return SCHEMA;
  }
}
