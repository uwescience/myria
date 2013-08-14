package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;

/**
 * An abstraction for a unary operator.
 * 
 * @author dhalperi
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
    return new Operator[] { child };
  }

  /**
   * @return the child.
   */
  public final Operator getChild() {
    return child;
  }

  @Override
  public final void setChildren(final Operator[] children) {
    Preconditions.checkArgument(child == null, "called setChildren(), but children have already been set");
    Preconditions.checkNotNull(children);
    Preconditions.checkArgument(children.length == 1, "setChildren() must be called with an array of length 1");
    Preconditions.checkNotNull(children[0]);
    child = children[0];
  }

}
