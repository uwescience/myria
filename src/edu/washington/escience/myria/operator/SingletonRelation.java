package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * A relation of exactly one row. In principle, it should have no columns, but we require there to be at least one.
 *
 */
public final class SingletonRelation extends LeafOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The Schema of the tuples output by this operator. */
  private static final Schema SINGLETON_SCHEMA =
      Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("privateSingletonColumn"));
  /** The data in this relation. */
  private TupleBatch tuples;

  /**
   * Constructs a relation with exactly one row.
   */
  public SingletonRelation() {
    TupleBatchBuffer tbb = new TupleBatchBuffer(SINGLETON_SCHEMA);
    tbb.putInt(0, 0);
    tuples = tbb.popAny();
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch tb = tuples;
    tuples = null;
    return tb;
  }

  @Override
  protected Schema generateSchema() {
    return SINGLETON_SCHEMA;
  }
}
