package edu.washington.escience.myria.operator.agg;

import java.util.List;

import com.google.common.math.LongMath;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An aggregator that counts the number of rows in its input.
 */
public final class CountAllAggregator implements Aggregator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The schema of the aggregate results. */
  public static final Schema SCHEMA = Schema.ofFields(Type.LONG_TYPE, "count_all");

  @Override
  public void add(final ReadableTable from, final Object state) throws DbException {
    CountAllState c = (CountAllState) state;
    c.count = LongMath.checkedAdd(c.count, from.numTuples());
  }

  @Override
  public void addRow(final ReadableTable from, final int row, final Object state)
      throws DbException {
    CountAllState c = (CountAllState) state;
    c.count = LongMath.checkedAdd(c.count, 1);
  }

  @Override
  public void getResult(final AppendableTable dest, final int destColumn, final Object state)
      throws DbException {
    CountAllState c = (CountAllState) state;
    dest.putLong(destColumn, c.count);
  }

  @Override
  public Schema getResultSchema() {
    return SCHEMA;
  }

  @Override
  public Object getInitialState() {
    return new CountAllState();
  }

  /** Private internal class that wraps the state required by this Aggregator as an object. */
  private final class CountAllState {
    /** The number of tuples seen so far. */
    private long count = 0;
  }

  @Override
  public void add(final List<TupleBatch> from, final Object state) throws DbException {
    throw new DbException(" method not implemented");
  }
}
