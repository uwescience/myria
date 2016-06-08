package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;

/**
 * An abstraction for a unary operator.
 *
 *
 */
public abstract class UnaryOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The child.
   */
  private Operator child;

  /**
   * @param child the single child of this operator.
   */
  public UnaryOperator(final Operator child) {
    this.child = child;
  }

  @Override
  public final Operator[] getChildren() {
    if (child == null) {
      return null;
    }
    return new Operator[] {child};
  }

  /**
   * @return the child.
   */
  public final Operator getChild() {
    return child;
  }

  /**
   * @param child the child.
   */
  public final void setChild(final Operator child) {
    setChildren(new Operator[] {child});
  }

  @Override
  public final void setChildren(final Operator[] children) {
    Integer opId = getOpId();
    Preconditions.checkArgument(
        child == null,
        "Operator opid=%s called setChildren(), but children have already been set",
        opId);
    Preconditions.checkNotNull(children, "Unary operator opId=%s has null children", opId);
    Preconditions.checkArgument(
        children.length == 1,
        "Operator opId=%s setChildren() must be called with an array of length 1",
        opId);
    Preconditions.checkNotNull(
        children[0], "Unary operator opId=%s has its child to be null", opId);
    child = children[0];
  }
}
