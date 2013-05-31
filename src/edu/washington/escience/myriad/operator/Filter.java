package edu.washington.escience.myriad.operator;

import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Filter is an operator that implements a relational select.
 */
public final class Filter extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The operator.
   * */
  private final Predicate predicate;
  /**
   * the child.
   * */
  private Operator child;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   * 
   * @param predicate the predicate by which to filter tuples.
   * @param child The child operator
   */
  public Filter(final Predicate predicate, final Operator child) {
    this.predicate = predicate;
    this.child = child;
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tmp = null;

    while ((tmp = child.next()) != null) {
      if (tmp.numTuples() > 0) {
        tmp = tmp.filter(predicate);
        if (tmp.numTuples() > 0) {
          return tmp;
        }
      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tmp = null;
    tmp = child.nextReady();
    while (tmp != null) {
      // tmp = child.next();
      tmp = tmp.filter(predicate);
      if (tmp.numTuples() > 0) {
        return tmp;
      }
      tmp = child.nextReady();
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
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException, NoSuchElementException {
    // need no init
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
