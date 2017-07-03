package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.ConcatColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An abstract class used to make those specialized operators that only consume tuples simpler to implement.
 *
 *
 */
public abstract class RootOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Source of the tuples to be consumed. */
  private Operator child;
  /** The coalescing threshold. 0 or negative means do not coalesce. */
  private int threshold;

  /**
   * Sets important parameters for successful operation.
   *
   * @param child the source of tuples that this Root operator consumes.
   */
  public RootOperator(final Operator child) {
    this.child = child;
  }

  /**
   * Set threshold for the root operator, only used by DBInsert and DBIsertTemp operatos.
   * @param threshold size of tuplebatch
   */
  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  /**
   * Perform the function of this operator on the provided tuples. For instance, may print the tuples to the screen or
   * write them to disk.
   *
   * @param tuples the tuples to be consumed.
   * @throws DbException if there's an error in the database.
   */
  protected abstract void consumeTuples(TupleBatch tuples) throws DbException;

  /**
   * If the child EOS is meet, the method is called back to let the root operators deal with this event.
   *
   * @throws DbException if any error occurs.
   * */
  protected abstract void childEOS() throws DbException;

  /**
   * call if the child meets EOI.
   *
   * @throws DbException if any error occurs.
   * */
  protected abstract void childEOI() throws DbException;

  /**
   * Implement coalescing tuples together if necessary.
   *
   * @param first the first, non-null tuple batch to be handled.
   * @return a coalesced tuple batch consisting of either all ready tuples or at least {@link #threshold} tuples.
   * @throws DbException if there is an error.
   */
  private TupleBatch getCoalesced(@Nonnull final TupleBatch first) throws DbException {
    if (first.numTuples() > threshold) {
      return first;
    }
    TupleBatch next = child.nextReady();
    if (next == null) {
      return first;
    }

    /* Start a concat column of the first two batches we found. */
    List<ConcatColumn<?>> cols = new ArrayList<>(first.numColumns());
    for (Column<?> c : first.getDataColumns()) {
      ConcatColumn<?> col = new ConcatColumn<>(c.getType());
      col.addColumn(c);
      cols.add(col);
    }
    int i = 0;
    for (Column<?> c : next.getDataColumns()) {
      cols.get(i).addColumn(c);
      ++i;
    }

    while (cols.get(0).size() < threshold) {
      next = child.nextReady();
      if (next == null) {
        break;
      }
      i = 0;
      for (Column<?> c : next.getDataColumns()) {
        cols.get(i).addColumn(c);
        ++i;
      }
    }
    return new TupleBatch(getSchema(), cols);
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    tb = child.nextReady();
    if (tb != null) {
      tb = getCoalesced(tb);
      consumeTuples(tb);
    } else if (child.eoi()) {
      childEOI();
    } else if (child.eos()) {
      childEOS();
    }
    return tb;
  }

  /**
   * @return the source of the tuples that this Root operator consumes.
   */
  public final Operator getChild() {
    return child;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] {child};
  }

  @Override
  public final void setChildren(final Operator[] children) {
    if (children.length != 1) {
      throw new IllegalArgumentException("a root operator must have exactly one child");
    }
    Preconditions.checkNotNull(children[0], "RootOperator opId=%s has a null child", getOpId());
    child = children[0];
  }

  @Override
  public final Schema generateSchema() {
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  /**
   * process EOS and EOI logic.
   * */
  @Override
  protected void checkEOSAndEOI() {
    if (child.eos()) {
      setEOS();
    } else if (child.eoi()) {
      setEOI(true);
      child.setEOI(false);
    }
  }
}
