package edu.washington.escience.myria.operator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A poor implementation of a Limit operator, which emits the first N tuples then closes the child operator from further
 * feeding tuples.
 * 
 */
public final class Limit extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The limit. */
  private final long limit;

  /** The number of tuples left to emit. */
  private long toEmit;

  /** If number of emitted tuples reached limit. */
  private boolean done;

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
    done = false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    Operator child = getChild();
    if (done) {
      return null;
    } else {
      TupleBatch tb = child.nextReady();
      TupleBatch result = null;
      if (tb != null) {
        if (tb.numTuples() <= toEmit) {
          toEmit -= tb.numTuples();
          result = tb;
        } else if (toEmit > 0) {
          result = tb.prefix(Ints.checkedCast(toEmit));
          toEmit = 0;
          child.close();
          done = true;
        }
      }
      return result;
    }
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
