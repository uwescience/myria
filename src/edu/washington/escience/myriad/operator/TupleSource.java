package edu.washington.escience.myriad.operator;

import java.util.List;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * This class creates a LeafOperator from a batch of tuples. Useful for testing.
 * 
 * @author dhalperi
 * 
 */
public final class TupleSource extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The tuples that this operator serves, exactly once. */
  private final List<TupleBatch> data;
  /** The Schema of the tuples that this operator serves. */
  private final Schema schema;

  /**
   * Constructs a TupleSource operator that will serve the tuples in the given TupleBatchBuffer.
   * 
   * @param data the tuples that this operator will serve.
   */
  public TupleSource(final TupleBatchBuffer data) {
    this.data = data.getAll();
    schema = data.getSchema();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (data.isEmpty()) {
      return null;
    }
    return data.remove(0);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
