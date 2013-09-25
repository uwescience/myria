package edu.washington.escience.myria.operator;

import java.util.Objects;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;

public abstract class NAryOperator extends Operator {

  /**
   * The union children. It is required that the schemas of all the children are the same.
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
    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
    }
    this.children = children;
  }

  @Override
  public Schema getSchema() {
    return children[0].getSchema();
  }

}