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
public final class Filter extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The operator.
   * */
  private final Predicate predicate;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   * 
   * @param predicate the predicate by which to filter tuples.
   * @param child The child operator
   */
  public Filter(final Predicate predicate, final Operator child) {
    super(child);
    this.predicate = predicate;
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tmp = null;
    tmp = getChild().nextReady();
    while (tmp != null) {
      // tmp = child.next();
      tmp = tmp.filter(predicate);
      if (tmp.numTuples() > 0) {
        return tmp;
      }
      tmp = getChild().nextReady();
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return getChild().getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException, NoSuchElementException {
    // need no init
  }
}
