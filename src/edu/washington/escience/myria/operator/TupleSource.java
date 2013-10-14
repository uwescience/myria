package edu.washington.escience.myria.operator;

import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
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
   * Constructs a TupleSource operator that will serve the tuples in the given List<TupleBatch>. Data must contain at
   * least one TupleBatch.
   * 
   * @param data the tuples that this operator will serve.
   */

  public TupleSource(final List<TupleBatch> data) {
    this(data, null);
  }

  /**
   * Constructs a TupleSource operator that will serve the tuples in the given List<TupleBatch>.
   * 
   * @param data the tuples that this operator will serve.
   * @param schema the schema of the tuples.
   */
  public TupleSource(final List<TupleBatch> data, final Schema schema) {
    this.data = Objects.requireNonNull(data);
    if (data.size() == 0) {
      this.schema = Objects.requireNonNull(schema, "either data.get(0) must be non-null, or schema must be supplied");
    } else {
      this.schema = data.get(0).getSchema();
      if (schema != null) {
        Preconditions.checkArgument(this.schema.equals(schema),
            "supplied schema does not match the schema in data.get(0)");
      }
    }
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
  protected Schema generateSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    index = 0;
  }

  @Override
  protected void checkEOSAndEOI() {
    // do nothing since already done in fetchNextReady()
  }
}
