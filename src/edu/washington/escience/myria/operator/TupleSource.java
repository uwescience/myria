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
  private Schema schema;

  /**
   * Constructs a TupleSource operator that will serve the tuples in the given TupleBatchBuffer.
   * 
   * @param data the tuples that this operator will serve.
   */
  public TupleSource(final TupleBatchBuffer data) {
    this.data = data.getAll();
    schema = data.getSchema();
  }

  /**
   * Constructs a TupleSource operator that will serve the tuples in the given List<TupleBatch>.
   * 
   * @param data the tuples that this operator will serve.
   */

  public TupleSource(final List<TupleBatch> data) {
    this.data = data;
  }

  @Override
  protected void cleanup() throws DbException {
    index = 0;
    data.clear();
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (index >= data.size()) {
      setEOS();
      return null;
    }
    TupleBatch ret = data.get(index++);
    if (ret.isEOI()) {
      setEOI(true);
      return null;
    }
    return ret;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    index = 0;
  }

  /** @param schema the schema to be set . */
  public void setSchema(final Schema schema) {
    this.schema = schema;
  }
}
