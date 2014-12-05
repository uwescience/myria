package edu.washington.escience.myria.operator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A poor implementation of a Limit operator, which emits the first N tuples then drops the rest on the floor.
 * 
 * We would prefer one that stops the incoming stream, but this is not currently supported.
 */
public final class Limit extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The limit. */
  private final long limit;

  /** The number of tuples left to emit. */
  private long toEmit;

  /**
   * A limit operator keeps the first <code>limit</code> tuples produced by its child.
   * 
   * @param limit the number of tuples to keep.
   * @param child the source of tuples.
   */
  public Limit(@Nonnull final Long limit, final Operator child) {
    super(child);
    this.limit = Objects.requireNonNull(limit, "limit");
    Preconditions.checkArgument(limit >= 0L, "limit must be non-negative");
    toEmit = this.limit;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    Operator child = getChild();
    for (TupleBatch tb = child.nextReady(); tb != null; tb = child.nextReady()) {
      if (tb.numTuples() <= toEmit) {
        toEmit -= tb.numTuples();
        return tb;
      } else if (toEmit > 0) {
        tb = tb.prefix(Ints.checkedCast(toEmit));
        toEmit = 0;
        return tb;
      }
      /* Else, drop on the floor. */
    }
    return null;
  }

  @Override
  public Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }
}
