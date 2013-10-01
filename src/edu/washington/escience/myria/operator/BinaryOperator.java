package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;

/**
 * An abstraction for a binary operator.
 * 
 * @author dhalperi
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
    Preconditions.checkArgument((left == null && right == null) || (left != null && right != null),
        "Must be constructed with either two null children or two instantiated children.");
    this.left = left;
    this.right = right;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { left, right };
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
    Preconditions.checkArgument(left == null && right == null,
        "called setChildren(), but children have already been set");
    Preconditions.checkNotNull(children);
    Preconditions.checkArgument(children.length == 2, "setChildren() must be called with an array of length 2");
    Preconditions.checkNotNull(children[0]);
    Preconditions.checkNotNull(children[1]);
    left = children[0];
    right = children[1];
    /* Generate the Schema now as a way of sanity-checking the constructor arguments. */
    getSchema();
  }

}
