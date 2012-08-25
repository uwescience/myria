package edu.washington.escience.parallel;

import java.util.NoSuchElementException;

import edu.washington.escience.Predicate;
import edu.washington.escience.Schema;
import edu.washington.escience.TupleBatch;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;
  // <silentstrip lab1|lab2>
  private Predicate.Op op;
  private Object operand;
  private int fieldIdx;
  private Operator child;

  // </silentstrip>

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   * 
   * @param p The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate.Op op, int fieldIdx, Object operand, Operator child) {
    // <strip lab1|lab2>
    // this.pred = p;
    this.op = op;
    this.fieldIdx = fieldIdx;
    this.operand = operand;
    this.child = child;
    // </strip>
  }

  // public Predicate getPredicate() {
  // // <strip lab1|lab2>
  // return pred;
  // // </strip>
  // // <insert lab1|lab2>
  // // return null;
  // // </insert>
  // }

  @Override
  public void close() {
    // <strip lab1|lab2>
    super.close();
    child.close();
    // </strip>
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
  protected TupleBatch fetchNext() throws NoSuchElementException, DbException {
    // <strip lab1|lab2>
    while (child.hasNext()) {
      return child.next().filter(this.fieldIdx, this.op, this.operand);
    }
    return null;
    // </strip>
    // <insert lab1|lab2>
    // return null;
    // </insert>
  }

  @Override
  public Operator[] getChildren() {
    // <strip lab1|lab2>
    return new Operator[] {this.child};
    // </strip>
    // <insert lab1|lab2>
    // return null;
    // </insert>
  }

  @Override
  public Schema getSchema() {
    // <strip lab1|lab2>
    return child.getSchema();
    // </strip>
    // <insert lab1|lab2>
    // return null;
    // </insert>
  }

  @Override
  public void open() throws DbException, NoSuchElementException {
    // <strip lab1|lab2>
    child.open();
    super.open();
    // </strip>
  }

  @Override
  public void rewind() throws DbException {
    // <strip lab1|lab2>
    child.rewind();
    // </strip>
  }

  @Override
  public void setChildren(Operator[] children) {
    // <strip lab1|lab2>
    this.child = children[0];
    // </strip>
  }

}
