package edu.washington.escience.myriad.parallel;

import java.util.NoSuchElementException;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private final Predicate.Op op;
  private final Object operand;
  private final int fieldIdx;
  private Operator child;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   * 
   * @param p The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate.Op op, int fieldIdx, Object operand, Operator child) {
    this.op = op;
    this.fieldIdx = fieldIdx;
    this.operand = operand;
    this.child = child;
  }

  @Override
  public void close() {
    super.close();
    child.close();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator,
   * applying the predicate to them and returning those that pass the predicate (i.e. for which the
   * Predicate.filter() returns true.)
   * 
   * @return The next tuple that passes the filter, or null if there are no more tuples
   * @see Predicate#filter
   */
  @Override
  protected _TupleBatch fetchNext() throws NoSuchElementException, DbException {
    if (child.hasNext()) {
      return child.next().filter(this.fieldIdx, this.op, this.operand);
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] {this.child};
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void open() throws DbException, NoSuchElementException {
    child.open();
    super.open();
  }

  // @Override
  // public void rewind() throws DbException {
  // child.rewind();
  // }

  @Override
  public void setChildren(Operator[] children) {
    this.child = children[0];
  }

}
