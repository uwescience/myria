package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;

/**
 * An abstraction for a binary operator.
 *
 *
 */
public abstract class BinaryOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The left child. */
  private Operator left;
  /** The right child. */
  private Operator right;

  /**
   * @param left the left child of this operator.
   * @param right the left child of this operator.
   */
  public BinaryOperator(final Operator left, final Operator right) {
    Preconditions.checkArgument(
        (left == null && right == null) || (left != null && right != null),
        "Must be constructed with either two null children or two instantiated children.");
    this.left = left;
    this.right = right;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] {left, right};
  }

  /**
   * @return the left child.
   */
  public final Operator getLeft() {
    return left;
  }

  /**
   * @return the right child.
   */
  public final Operator getRight() {
    return right;
  }

  @Override
  public final void setChildren(final Operator[] children) {
    Integer opId = getOpId();
    Preconditions.checkArgument(
        left == null && right == null,
        "Operator opId=%s called setChildren(), but children have already been set",
        opId);
    Preconditions.checkNotNull(children, "Operator opId=%s has null children", opId);
    Preconditions.checkArgument(
        children.length == 2,
        "Operator opId=%s setChildren() must be called with an array of length 2",
        opId);
    Preconditions.checkNotNull(
        children[0], "Operator opId=%s has its first child to be null", opId);
    Preconditions.checkNotNull(
        children[1], "Operator opId=%s has its second child to be null", opId);
    left = children[0];
    right = children[1];
  }
}
