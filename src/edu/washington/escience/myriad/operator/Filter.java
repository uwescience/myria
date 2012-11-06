package edu.washington.escience.myriad.operator;

import java.util.NoSuchElementException;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Filter is an operator that implements a relational select.
 */
public final class Filter extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private final Predicate.Op op;
  private final Object operand;
  private final int fieldIdx;
  private Operator child;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   * 
   * @param child The child operator
   */
  public Filter(final Predicate.Op op, final int fieldIdx, final Object operand, final Operator child) {
    this.op = op;
    this.fieldIdx = fieldIdx;
    this.operand = operand;
    this.child = child;
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator, applying the predicate to
   * them and returning those that pass the predicate (i.e. for which the Predicate.filter() returns true.)
   * 
   * @return The next tuple that passes the filter, or null if there are no more tuples
   * @see Predicate#filter
   */
  @Override
  protected _TupleBatch fetchNext() throws NoSuchElementException, DbException {
    _TupleBatch tmp = null;

    while ((tmp = child.next()) != null) {
      if (tmp.numOutputTuples() > 0) {
        tmp = tmp.filter(fieldIdx, op, operand);
        if (tmp.numOutputTuples() > 0) {
          return tmp;
        }
      }
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init() throws DbException, NoSuchElementException {
    // need no init
  }

  // @Override
  // public void rewind() throws DbException {
  // child.rewind();
  // }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    _TupleBatch tmp = null;
    if (child.nextReady()) {
      tmp = child.next();
      tmp = tmp.filter(fieldIdx, op, operand);
      if (tmp.numOutputTuples() > 0) {
        return tmp;
      }
    }
    return null;
  }

}
