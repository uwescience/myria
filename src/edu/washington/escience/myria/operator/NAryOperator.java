package edu.washington.escience.myria.operator;

import java.util.Objects;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;

public abstract class NAryOperator extends Operator {

  /**
   * The children of the n-ary operator.
   * */
  protected Operator[] children;

  public NAryOperator() {
    super();
  }

  @Override
  public Operator[] getChildren() {
    return children;
  }

  @Override
  public void setChildren(final Operator[] children) {
    Objects.requireNonNull(children);
    Preconditions.checkArgument(children.length > 0);
    this.children = children;
  }

  @Override
  public Schema getSchema() {
    return children[0].getSchema();
  }

  /**
   * @return number of children
   */
  protected int numChildren() {
    return children.length;
  }

}