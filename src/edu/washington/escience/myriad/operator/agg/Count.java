package edu.washington.escience.myriad.operator.agg;

import java.util.Objects;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Count the number of tuples from a child operator.
 * 
 * @author dhalperi
 * 
 */
public final class Count extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The schema of the results. */
  private final Schema schema;
  /** The source of tuples to be aggregated. */
  private Operator child;
  /** The total number of tuples. */
  private long count = 0;

  /**
   * Construct an operator that will count the number of tuples produced by the given child.
   * 
   * @param child the source of tuples to be counted.
   */
  public Count(final Operator child) {
    Objects.requireNonNull(child);
    schema = new Schema(new Type[] { Type.LONG_TYPE }, new String[] { "COUNT" });
    this.child = child;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {

    _TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      count += tb.numOutputTuples();
    }
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.put(0, count);
    _TupleBatch result = tbb.popAny();
    setEOS();
    return result;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
    count = 0;
  }

  @Override
  protected _TupleBatch fetchNextReady() throws DbException {
    if (child.eos()) {
      return fetchNext();
    }
    return null;
  }

}
