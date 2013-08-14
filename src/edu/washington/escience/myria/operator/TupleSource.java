package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;

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
  /**
   * the current TupleBatch index of this TupleSource. Does not remove the TupleBatches in execution so that it can
   * rewinded.
   * */
  private int index;
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
  protected void cleanup() throws DbException {
    index = 0;
    data.clear();
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (index >= data.size()) {
      return null;
    }
    return data.get(index++);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    index = 0;
  }

}
